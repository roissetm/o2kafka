# Oracle to PostgreSQL Transaction Atomicity

This document explains how the O2Kafka project preserves Oracle transaction atomicity when replicating data to PostgreSQL using Debezium CDC.

## The Problem

When using Debezium to capture changes from Oracle, each table's events go to separate Kafka topics/partitions:

```
Oracle Transaction:
┌─────────────────────────────────────────┐
│ INSERT orders      → orders-topic       │
│ INSERT order_items → order_items-topic  │
│ UPDATE inventory   → inventory-topic    │
│ INSERT payments    → payments-topic     │
│ COMMIT                                  │
└─────────────────────────────────────────┘
```

**Problems without transaction grouping:**
- Events consumed at different times
- Consumer crash = partial application
- No ordering guarantee across partitions
- Foreign key violations possible

## The Solution: Transaction Metadata

### Debezium Configuration

The connector must have these settings enabled:

```json
{
  "provide.transaction.metadata": "true",
  "log.mining.query.filter.mode": "in",
  "internal.log.mining.use.cte.query": "true"
}
```

| Property | Purpose |
|----------|---------|
| `provide.transaction.metadata` | Adds transaction block to events + creates transaction topic |
| `log.mining.query.filter.mode` | Must be `"in"` for CTE query feature |
| `internal.log.mining.use.cte.query` | Enables CTE query optimization |

### What Transaction Metadata Provides

**1. Transaction Topic** (`o2k.transaction`)

BEGIN marker:
```json
{
  "status": "BEGIN",
  "id": "040011009e020000",
  "event_count": null
}
```

END marker:
```json
{
  "status": "END",
  "id": "040011009e020000",
  "event_count": 8,
  "data_collections": [
    {"data_collection": "ECOM.ORDERS", "event_count": 1},
    {"data_collection": "ECOM.ORDER_ITEMS", "event_count": 3},
    {"data_collection": "ECOM.INVENTORY", "event_count": 3},
    {"data_collection": "ECOM.PAYMENTS", "event_count": 1}
  ]
}
```

**2. Transaction Block in Each Event**
```json
{
  "op": "c",
  "after": { ... },
  "source": { "table": "ORDERS", ... },
  "transaction": {
    "id": "040011009e020000",
    "total_order": 1,
    "data_collection_order": 1
  }
}
```

## CTE Query Feature

### Why It's Needed

In high-volume environments, Debezium receives millions of transaction markers daily - even for tables not being captured. This creates:

- Unnecessary network traffic
- CPU overhead processing irrelevant events
- Memory/GC pressure

### How It Works

Two-pass filtering at the database level:

```sql
-- Pass 1: Build CTE with XIDs for captured tables only
WITH relevant_txns AS (
  SELECT DISTINCT XID FROM V$LOGMNR_CONTENTS
  WHERE TABLE_NAME IN ('ORDERS', 'INVENTORY', ...)
)

-- Pass 2: Return only events matching those XIDs
SELECT * FROM V$LOGMNR_CONTENTS
WHERE XID IN (SELECT XID FROM relevant_txns)
```

### Trade-offs

| Benefit | Cost |
|---------|------|
| Reduced network traffic | Increased database I/O (two passes) |
| Lower connector CPU | May not help low-volume systems |
| Less memory/GC pressure | No benefit if capturing most tables |

## TX-Aggregator Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                           Kafka                                       │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌────────────────┐  │
│  │ orders      │ │ order_items │ │ inventory   │ │ o2k.transaction│  │
│  │ topic       │ │ topic       │ │ topic       │ │ topic          │  │
│  └──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └───────┬────────┘  │
└─────────┼───────────────┼───────────────┼────────────────┼───────────┘
          │               │               │                │
          └───────────────┴───────┬───────┴────────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │ TransactionAggregatorSvc  │
                    │                           │
                    │ • Group events by txId    │
                    │ • Wait for END marker     │
                    │ • Sort by total_order     │
                    │ • Detect completion       │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │   TransactionBundle       │
                    │                           │
                    │ txId: "040011009e020000"  │
                    │ events: [sorted by order] │
                    │ expectedCount: 8          │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │   PostgresSinkService     │
                    │                           │
                    │ • @Transactional          │
                    │ • Idempotency check       │
                    │ • Apply all events        │
                    │ • Record transaction      │
                    └─────────────┬─────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │       PostgreSQL          │
                    │                           │
                    │ • Atomic application      │
                    │ • cdc_transactions table  │
                    │ • All-or-nothing commit   │
                    └───────────────────────────┘
```

## Key Components

### TransactionBundle

Aggregates all events for a single Oracle transaction:

```java
public class TransactionBundle {
    private final String txId;
    private final Set<ChangeEvent> events;  // ConcurrentSkipListSet (sorted)
    private volatile int expectedCount;

    public boolean isComplete() {
        return endMarkerReceived && events.size() >= expectedCount;
    }

    public List<ChangeEvent> getOrderedEvents() {
        return new ArrayList<>(events);  // Already sorted by total_order
    }
}
```

### PostgresSinkService

Applies complete transactions atomically:

```java
@Transactional
public void applyTransaction(TransactionBundle bundle) {
    // 1. Idempotency check
    if (isTransactionAlreadyApplied(bundle.getTxId())) {
        return;  // Skip duplicate
    }

    // 2. Apply all events in order
    for (ChangeEvent event : bundle.getOrderedEvents()) {
        applyEvent(event);  // INSERT/UPDATE/DELETE
    }

    // 3. Record for idempotency
    recordTransaction(bundle);
}
```

### Idempotency Table

```sql
CREATE TABLE ecom.cdc_transactions (
    tx_id VARCHAR(64) PRIMARY KEY,
    event_count INTEGER,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Failure Scenarios

| Scenario | Behavior |
|----------|----------|
| Consumer crash mid-transaction | Bundle incomplete, not applied. Reprocessed on restart. |
| PostgreSQL failure during apply | @Transactional rollback. Retried. |
| Duplicate transaction received | Idempotency check skips it. |
| Event arrives before BEGIN | Bundle created implicitly, waits for END. |
| END arrives before all events | Bundle waits until event count matches. |
| Transaction timeout | Logged, sent to dead-letter (future). |

## Configuration Reference

### Debezium Connector (oracle-ecom-connector.json)

```json
{
  "provide.transaction.metadata": "true",
  "log.mining.query.filter.mode": "in",
  "internal.log.mining.use.cte.query": "true",
  "tombstones.on.delete": "false"
}
```

### TX-Aggregator (application.yml)

```yaml
aggregator:
  schema:
    target: ecom
  transaction:
    timeout-minutes: 5
    cleanup-interval-seconds: 60
```

## References

- [Debezium Oracle Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/oracle.html)
- [CTE Query Feature Blog Post](https://debezium.io/blog/2025/08/14/oracle-new-feature-cte-query/)
- [Transaction Metadata Documentation](https://debezium.io/documentation/reference/stable/connectors/oracle.html#oracle-transaction-metadata)
