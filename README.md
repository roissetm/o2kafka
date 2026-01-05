# O2Kafka: Oracle to PostgreSQL CDC with Transaction Boundaries

Demonstrates preserving Oracle transaction atomicity when replicating to PostgreSQL via Debezium and Kafka, using the **CTE (Common Table Expression) query feature**.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Oracle 21c    │────▶│  Kafka Connect  │────▶│     Kafka       │
│   (10 tables)   │     │   (Debezium)    │     │  (10 + 1 topics)│
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                         │
                                                         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  PostgreSQL 17  │◀────│   Transaction   │◀────│  CDC Events +   │
│   (10 tables)   │     │   Aggregator    │     │  TX Metadata    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Quick Start

### 1. Start Infrastructure

```bash
# Start all services
docker-compose up -d

# Wait for services to be ready (Oracle takes ~2 minutes)
docker-compose logs -f oracle  # Wait for "DATABASE IS READY"
```

### 2. Deploy Debezium Connector

```bash
# Deploy the Oracle CDC connector
./scripts/deploy-connector.sh
```

### 3. Verify Replication

```bash
# Check connector status and topic counts
./scripts/verify-replication.sh
```

## Services

| Service | Port | URL/Connection |
|---------|------|----------------|
| Oracle 21c | 1521 | `system/oracle_pwd@localhost:1521/XEPDB1` |
| PostgreSQL 17 | 5432 | `postgres:postgres@localhost:5432/ecom` |
| Kafka | 29092 | `localhost:29092` |
| Kafka Connect | 8083 | http://localhost:8083 |
| Schema Registry | 8081 | http://localhost:8081 |
| Kafka UI | 8080 | http://localhost:8080 |

## Key Configuration

### Debezium CTE Query Feature

The connector uses the CTE query mode for improved LogMiner performance:

```json
{
  "log.mining.query.filter.mode": "cte",
  "provide.transaction.metadata": "true"
}
```

### Transaction Metadata

Each CDC event includes transaction context:

```json
{
  "source": {
    "txId": "6.28.2066",
    "scn": "1234567890"
  },
  "transaction": {
    "id": "6.28.2066",
    "total_order": 1,
    "data_collection_order": 1
  }
}
```

The `o2k.transaction` topic receives BEGIN/END markers:

```json
{
  "status": "END",
  "id": "6.28.2066",
  "event_count": 5,
  "data_collections": [
    {"data_collection": "ECOM.ORDERS", "event_count": 1},
    {"data_collection": "ECOM.ORDER_ITEMS", "event_count": 2}
  ]
}
```

## E-Commerce Schema

10 tables modeling a realistic e-commerce system:

1. **categories** - Product categories (hierarchical)
2. **products** - Product catalog
3. **inventory** - Stock levels per warehouse
4. **customers** - Customer accounts
5. **orders** - Customer orders
6. **order_items** - Line items
7. **payments** - Payment transactions
8. **shipments** - Shipping info
9. **shipment_items** - Items per shipment
10. **returns** - Product returns

## Testing Transaction Boundaries

### Create a Multi-Table Transaction in Oracle

```sql
-- Connect to Oracle
sqlplus ecom/ecom_pwd@localhost:1521/XEPDB1

-- Execute multi-table transaction
BEGIN
    INSERT INTO orders (order_id, customer_id, status, total_amount)
    VALUES (ecom.seq_orders.NEXTVAL, 1, 'CONFIRMED', 1099.98);

    INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price)
    VALUES (ecom.seq_order_items.NEXTVAL, ecom.seq_orders.CURRVAL, 101, 1, 999.99);

    INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price)
    VALUES (ecom.seq_order_items.NEXTVAL, ecom.seq_orders.CURRVAL, 106, 1, 99.99);

    UPDATE inventory SET quantity = quantity - 1
    WHERE product_id = 101 AND warehouse_code = 'WH01';

    INSERT INTO payments (payment_id, order_id, amount, method, status)
    VALUES (ecom.seq_payments.NEXTVAL, ecom.seq_orders.CURRVAL, 1099.98, 'CREDIT_CARD', 'COMPLETED');

    COMMIT;
END;
/
```

### Verify in Kafka

```bash
# Watch transaction topic
kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic o2k.transaction \
  --from-beginning
```

Expected: All 5 events share the same `txId`, with an END marker showing `event_count: 5`.

## Project Structure

```
o2kafka/
├── docker-compose.yml          # Infrastructure definition
├── .env                        # Environment variables
├── docker/
│   ├── oracle/init/           # Oracle initialization scripts
│   ├── postgres/init/         # PostgreSQL schema
│   └── kafka-connect/         # Custom Connect image
├── connectors/                # Connector configurations
├── scripts/                   # Helper scripts
└── tests/                     # Test scenarios
```

## Useful Commands

```bash
# View all containers
docker-compose ps

# View connector status
curl -s http://localhost:8083/connectors/oracle-ecom-connector/status | jq

# List Kafka topics
kafka-topics --bootstrap-server localhost:29092 --list

# Consume from a topic
kafka-console-consumer --bootstrap-server localhost:29092 --topic o2k.ECOM.ORDERS --from-beginning

# Connect to Oracle
sqlplus ecom/ecom_pwd@localhost:1521/XEPDB1

# Connect to PostgreSQL
psql -h localhost -U postgres -d ecom
```

## Cleanup

```bash
# Stop and remove containers
docker-compose down

# Remove volumes (data)
docker-compose down -v
```

## References

- [Debezium Oracle Connector](https://debezium.io/documentation/reference/stable/connectors/oracle.html)
- [CTE Query Feature](https://debezium.io/blog/2025/08/14/oracle-new-feature-cte-query/)
- [Transaction Metadata](https://debezium.io/documentation/reference/stable/connectors/oracle.html#oracle-transaction-metadata)
