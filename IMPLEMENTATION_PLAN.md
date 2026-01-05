# O2Kafka: CDC Transaction Aggregation Implementation Plan

## Executive Summary

This project demonstrates preserving Oracle transaction atomicity when replicating to PostgreSQL via Debezium and Kafka. The key innovation is using Debezium's **CTE (Common Table Expression) query feature** combined with **transaction metadata** to aggregate CDC events by their original transaction boundaries.

---

## Architecture Overview

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

---

## Phase 1: Infrastructure Setup

### 1.1 Docker Compose Services

| Service | Image | Purpose |
|---------|-------|---------|
| `oracle` | `gvenzl/oracle-xe:21-slim` | Source database |
| `zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | Kafka coordination |
| `kafka` | `confluentinc/cp-kafka:7.5.0` | Message broker |
| `schema-registry` | `confluentinc/cp-schema-registry:7.5.0` | Avro schema management |
| `kafka-connect` | `debezium/connect:3.0` | CDC connector runtime |
| `postgres` | `postgres:17` | Target database |
| `kafka-ui` | `provectuslabs/kafka-ui:latest` | Monitoring UI |
| `tx-aggregator` | Custom Java app | Transaction aggregation |

### 1.2 Network Configuration

```yaml
networks:
  o2kafka-net:
    driver: bridge
```

### 1.3 Volume Mounts

```yaml
volumes:
  oracle-data:      # Oracle persistence
  postgres-data:    # PostgreSQL persistence
  kafka-data:       # Kafka persistence
  connect-plugins:  # Debezium connectors
```

---

## Phase 2: Oracle Source Database

### 2.1 Schema Design: E-Commerce Domain

We'll model a realistic e-commerce system with 10 interrelated tables that naturally have multi-table transactions.

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  CUSTOMERS   │     │   PRODUCTS   │     │  CATEGORIES  │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       │    ┌───────────────┴────────────────────┘
       │    │
       ▼    ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│    ORDERS    │────▶│ ORDER_ITEMS  │     │  INVENTORY   │
└──────┬───────┘     └──────────────┘     └──────────────┘
       │
       ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   PAYMENTS   │     │  SHIPMENTS   │     │   RETURNS    │
└──────────────┘     └──────────────┘     └──────────────┘
                            │
                            ▼
                     ┌──────────────┐
                     │SHIPMENT_ITEMS│
                     └──────────────┘
```

### 2.2 Table Definitions

```sql
-- 1. CATEGORIES
CREATE TABLE categories (
    category_id    NUMBER(10) PRIMARY KEY,
    name           VARCHAR2(100) NOT NULL,
    parent_id      NUMBER(10) REFERENCES categories(category_id),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. PRODUCTS
CREATE TABLE products (
    product_id     NUMBER(10) PRIMARY KEY,
    sku            VARCHAR2(50) UNIQUE NOT NULL,
    name           VARCHAR2(200) NOT NULL,
    description    CLOB,
    price          NUMBER(10,2) NOT NULL,
    category_id    NUMBER(10) REFERENCES categories(category_id),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. INVENTORY
CREATE TABLE inventory (
    inventory_id   NUMBER(10) PRIMARY KEY,
    product_id     NUMBER(10) REFERENCES products(product_id),
    warehouse_code VARCHAR2(10) NOT NULL,
    quantity       NUMBER(10) NOT NULL,
    reserved       NUMBER(10) DEFAULT 0,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, warehouse_code)
);

-- 4. CUSTOMERS
CREATE TABLE customers (
    customer_id    NUMBER(10) PRIMARY KEY,
    email          VARCHAR2(255) UNIQUE NOT NULL,
    first_name     VARCHAR2(100),
    last_name      VARCHAR2(100),
    phone          VARCHAR2(20),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. ORDERS
CREATE TABLE orders (
    order_id       NUMBER(10) PRIMARY KEY,
    customer_id    NUMBER(10) REFERENCES customers(customer_id),
    status         VARCHAR2(20) DEFAULT 'PENDING',
    total_amount   NUMBER(12,2),
    shipping_addr  VARCHAR2(500),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. ORDER_ITEMS
CREATE TABLE order_items (
    item_id        NUMBER(10) PRIMARY KEY,
    order_id       NUMBER(10) REFERENCES orders(order_id),
    product_id     NUMBER(10) REFERENCES products(product_id),
    quantity       NUMBER(5) NOT NULL,
    unit_price     NUMBER(10,2) NOT NULL,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 7. PAYMENTS
CREATE TABLE payments (
    payment_id     NUMBER(10) PRIMARY KEY,
    order_id       NUMBER(10) REFERENCES orders(order_id),
    amount         NUMBER(12,2) NOT NULL,
    method         VARCHAR2(20) NOT NULL,
    status         VARCHAR2(20) DEFAULT 'PENDING',
    transaction_ref VARCHAR2(100),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 8. SHIPMENTS
CREATE TABLE shipments (
    shipment_id    NUMBER(10) PRIMARY KEY,
    order_id       NUMBER(10) REFERENCES orders(order_id),
    carrier        VARCHAR2(50),
    tracking_num   VARCHAR2(100),
    status         VARCHAR2(20) DEFAULT 'PREPARING',
    shipped_at     TIMESTAMP,
    delivered_at   TIMESTAMP,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 9. SHIPMENT_ITEMS
CREATE TABLE shipment_items (
    shipment_item_id NUMBER(10) PRIMARY KEY,
    shipment_id      NUMBER(10) REFERENCES shipments(shipment_id),
    order_item_id    NUMBER(10) REFERENCES order_items(item_id),
    quantity         NUMBER(5) NOT NULL,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 10. RETURNS
CREATE TABLE returns (
    return_id      NUMBER(10) PRIMARY KEY,
    order_id       NUMBER(10) REFERENCES orders(order_id),
    order_item_id  NUMBER(10) REFERENCES order_items(item_id),
    quantity       NUMBER(5) NOT NULL,
    reason         VARCHAR2(500),
    status         VARCHAR2(20) DEFAULT 'REQUESTED',
    refund_amount  NUMBER(10,2),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2.3 Oracle LogMiner Configuration

```sql
-- Enable supplemental logging (required for Debezium)
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- Create Debezium user
CREATE USER debezium IDENTIFIED BY dbz_password;
GRANT CREATE SESSION TO debezium;
GRANT SELECT ON V_$DATABASE TO debezium;
GRANT FLASHBACK ANY TABLE TO debezium;
GRANT SELECT ANY TABLE TO debezium;
GRANT SELECT_CATALOG_ROLE TO debezium;
GRANT EXECUTE_CATALOG_ROLE TO debezium;
GRANT SELECT ANY TRANSACTION TO debezium;
GRANT LOGMINING TO debezium;
GRANT CREATE TABLE TO debezium;
GRANT ALTER ANY TABLE TO debezium;
GRANT CREATE SEQUENCE TO debezium;

-- Grant LogMiner privileges
GRANT SELECT ON V_$LOG TO debezium;
GRANT SELECT ON V_$LOG_HISTORY TO debezium;
GRANT SELECT ON V_$LOGMNR_LOGS TO debezium;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO debezium;
GRANT SELECT ON V_$LOGFILE TO debezium;
GRANT SELECT ON V_$ARCHIVED_LOG TO debezium;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO debezium;
```

---

## Phase 3: Kafka Configuration

### 3.1 Topic Structure

| Topic | Purpose | Partitions | Retention |
|-------|---------|------------|-----------|
| `o2k.ECOM.CATEGORIES` | Category changes | 1 | 7 days |
| `o2k.ECOM.PRODUCTS` | Product changes | 3 | 7 days |
| `o2k.ECOM.INVENTORY` | Inventory changes | 3 | 7 days |
| `o2k.ECOM.CUSTOMERS` | Customer changes | 3 | 7 days |
| `o2k.ECOM.ORDERS` | Order changes | 6 | 7 days |
| `o2k.ECOM.ORDER_ITEMS` | Order item changes | 6 | 7 days |
| `o2k.ECOM.PAYMENTS` | Payment changes | 3 | 7 days |
| `o2k.ECOM.SHIPMENTS` | Shipment changes | 3 | 7 days |
| `o2k.ECOM.SHIPMENT_ITEMS` | Shipment item changes | 3 | 7 days |
| `o2k.ECOM.RETURNS` | Return changes | 3 | 7 days |
| `o2k.transaction` | Transaction markers | 1 | 7 days |

### 3.2 Topic Creation Script

```bash
#!/bin/bash
KAFKA_BROKER="kafka:9092"
TOPICS=(
    "o2k.ECOM.CATEGORIES:1"
    "o2k.ECOM.PRODUCTS:3"
    "o2k.ECOM.INVENTORY:3"
    "o2k.ECOM.CUSTOMERS:3"
    "o2k.ECOM.ORDERS:6"
    "o2k.ECOM.ORDER_ITEMS:6"
    "o2k.ECOM.PAYMENTS:3"
    "o2k.ECOM.SHIPMENTS:3"
    "o2k.ECOM.SHIPMENT_ITEMS:3"
    "o2k.ECOM.RETURNS:3"
    "o2k.transaction:1"
)

for topic_config in "${TOPICS[@]}"; do
    IFS=':' read -r topic partitions <<< "$topic_config"
    kafka-topics --create \
        --bootstrap-server $KAFKA_BROKER \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor 1 \
        --config retention.ms=604800000
done
```

---

## Phase 4: Debezium Oracle Connector

### 4.1 Connector Configuration with CTE Query Feature

```json
{
  "name": "oracle-ecom-connector",
  "config": {
    "connector.class": "io.debezium.connector.oracle.OracleConnector",
    "tasks.max": "1",

    "database.hostname": "oracle",
    "database.port": "1521",
    "database.user": "debezium",
    "database.password": "dbz_password",
    "database.dbname": "XEPDB1",
    "database.pdb.name": "XEPDB1",

    "topic.prefix": "o2k",
    "schema.include.list": "ECOM",
    "table.include.list": "ECOM.CATEGORIES,ECOM.PRODUCTS,ECOM.INVENTORY,ECOM.CUSTOMERS,ECOM.ORDERS,ECOM.ORDER_ITEMS,ECOM.PAYMENTS,ECOM.SHIPMENTS,ECOM.SHIPMENT_ITEMS,ECOM.RETURNS",

    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-changes.ecom",

    "provide.transaction.metadata": "true",
    "tombstones.on.delete": "false",

    "log.mining.strategy": "online_catalog",
    "log.mining.query.filter.mode": "cte",
    "log.mining.batch.size.min": "1000",
    "log.mining.batch.size.max": "10000",
    "log.mining.batch.size.default": "5000",

    "snapshot.mode": "initial",
    "snapshot.locking.mode": "none",

    "decimal.handling.mode": "double",
    "time.precision.mode": "connect",

    "heartbeat.interval.ms": "10000",
    "poll.interval.ms": "1000",

    "errors.log.enable": "true",
    "errors.log.include.messages": "true",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true"
  }
}
```

### 4.2 Key Configuration Parameters Explained

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `provide.transaction.metadata` | `true` | Enables transaction BEGIN/END markers |
| `log.mining.query.filter.mode` | `cte` | **CTE query feature** - improves LogMiner performance |
| `log.mining.strategy` | `online_catalog` | Uses online catalog for DDL resolution |
| `tombstones.on.delete` | `false` | Prevents null tombstone records |

### 4.3 CTE Query Feature Benefits

The CTE (Common Table Expression) query mode (`log.mining.query.filter.mode=cte`) provides:

1. **Improved Performance**: Uses SQL CTEs to filter LogMiner results more efficiently
2. **Better Transaction Boundary Detection**: More accurate transaction grouping at the source
3. **Reduced Memory Usage**: Filters data earlier in the query pipeline
4. **Lower Database Load**: Less data transferred from Oracle redo logs

---

## Phase 5: Transaction Aggregator Service

### 5.1 Project Structure

```
tx-aggregator/
├── pom.xml
├── Dockerfile
├── src/main/java/com/o2kafka/aggregator/
│   ├── TxAggregatorApplication.java
│   ├── config/
│   │   ├── KafkaConfig.java
│   │   └── PostgresConfig.java
│   ├── model/
│   │   ├── ChangeEvent.java
│   │   ├── TransactionBundle.java
│   │   └── TransactionMarker.java
│   ├── service/
│   │   ├── TransactionAggregatorService.java
│   │   ├── PostgresSinkService.java
│   │   └── TransactionTimeoutService.java
│   └── consumer/
│       ├── CdcEventConsumer.java
│       └── TransactionMarkerConsumer.java
└── src/main/resources/
    └── application.yml
```

### 5.2 Core Classes

#### TransactionBundle.java

```java
package com.o2kafka.aggregator.model;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

public class TransactionBundle {
    private final String txId;
    private final Instant startTime;
    private final Set<ChangeEvent> events;
    private volatile boolean complete = false;
    private volatile int expectedCount = -1;
    private volatile Map<String, Integer> expectedPerTable;

    public TransactionBundle(String txId) {
        this.txId = txId;
        this.startTime = Instant.now();
        this.events = new ConcurrentSkipListSet<>(
            Comparator.comparingInt(ChangeEvent::getTotalOrder)
        );
    }

    public synchronized void addEvent(ChangeEvent event) {
        events.add(event);
    }

    public synchronized void markComplete(TransactionMarker endMarker) {
        this.expectedCount = endMarker.getEventCount();
        this.expectedPerTable = endMarker.getDataCollections();
        this.complete = (events.size() >= expectedCount);
    }

    public boolean isComplete() {
        return complete && events.size() == expectedCount;
    }

    public boolean isTimedOut(Duration timeout) {
        return Instant.now().isAfter(startTime.plus(timeout));
    }

    public String getTxId() { return txId; }
    public Instant getStartTime() { return startTime; }
    public Set<ChangeEvent> getEvents() { return Collections.unmodifiableSet(events); }
    public int getExpectedCount() { return expectedCount; }
}
```

#### ChangeEvent.java

```java
package com.o2kafka.aggregator.model;

import com.fasterxml.jackson.databind.JsonNode;

public class ChangeEvent implements Comparable<ChangeEvent> {
    private final String table;
    private final String operation;  // c=create, u=update, d=delete, r=read (snapshot)
    private final JsonNode before;
    private final JsonNode after;
    private final int totalOrder;
    private final int dataCollectionOrder;
    private final String txId;
    private final long scn;

    public ChangeEvent(JsonNode eventPayload) {
        JsonNode source = eventPayload.path("source");
        JsonNode transaction = eventPayload.path("transaction");

        this.table = source.path("table").asText();
        this.operation = eventPayload.path("op").asText();
        this.before = eventPayload.path("before");
        this.after = eventPayload.path("after");
        this.totalOrder = transaction.path("total_order").asInt();
        this.dataCollectionOrder = transaction.path("data_collection_order").asInt();
        this.txId = source.path("txId").asText();
        this.scn = source.path("scn").asLong();
    }

    @Override
    public int compareTo(ChangeEvent other) {
        return Integer.compare(this.totalOrder, other.totalOrder);
    }

    // Getters
    public String getTable() { return table; }
    public String getOperation() { return operation; }
    public JsonNode getBefore() { return before; }
    public JsonNode getAfter() { return after; }
    public int getTotalOrder() { return totalOrder; }
    public int getDataCollectionOrder() { return dataCollectionOrder; }
    public String getTxId() { return txId; }
    public long getScn() { return scn; }
}
```

#### TransactionMarker.java

```java
package com.o2kafka.aggregator.model;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;

public class TransactionMarker {
    private final String status;  // BEGIN or END
    private final String id;
    private final int eventCount;
    private final Map<String, Integer> dataCollections;

    public TransactionMarker(JsonNode payload) {
        this.status = payload.path("status").asText();
        this.id = payload.path("id").asText();
        this.eventCount = payload.path("event_count").asInt(0);
        this.dataCollections = parseDataCollections(payload.path("data_collections"));
    }

    private Map<String, Integer> parseDataCollections(JsonNode node) {
        Map<String, Integer> result = new HashMap<>();
        if (node.isArray()) {
            for (JsonNode item : node) {
                String collection = item.path("data_collection").asText();
                int count = item.path("event_count").asInt();
                result.put(collection, count);
            }
        }
        return result;
    }

    public String getStatus() { return status; }
    public String getId() { return id; }
    public int getEventCount() { return eventCount; }
    public Map<String, Integer> getDataCollections() { return dataCollections; }
}
```

#### TransactionAggregatorService.java

```java
package com.o2kafka.aggregator.service;

import com.o2kafka.aggregator.model.*;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class TransactionAggregatorService {
    private static final Logger log = LoggerFactory.getLogger(TransactionAggregatorService.class);

    private final ConcurrentHashMap<String, TransactionBundle> pendingTx = new ConcurrentHashMap<>();
    private final PostgresSinkService sinkService;
    private final MeterRegistry metrics;

    public TransactionAggregatorService(PostgresSinkService sinkService, MeterRegistry metrics) {
        this.sinkService = sinkService;
        this.metrics = metrics;
    }

    public void handleChangeEvent(ChangeEvent event) {
        String txId = event.getTxId();

        TransactionBundle bundle = pendingTx.computeIfAbsent(
            txId,
            TransactionBundle::new
        );

        bundle.addEvent(event);
        metrics.counter("cdc.events.received", "table", event.getTable()).increment();

        // Check if already marked complete and all events arrived
        if (bundle.isComplete()) {
            completeTransaction(txId);
        }
    }

    public void handleTransactionMarker(TransactionMarker marker) {
        if ("BEGIN".equals(marker.getStatus())) {
            // Pre-create bundle for faster event handling
            pendingTx.computeIfAbsent(marker.getId(), TransactionBundle::new);
            log.debug("Transaction BEGIN: {}", marker.getId());
            return;
        }

        if ("END".equals(marker.getStatus())) {
            TransactionBundle bundle = pendingTx.get(marker.getId());
            if (bundle != null) {
                bundle.markComplete(marker);
                log.debug("Transaction END: {} with {} events", marker.getId(), marker.getEventCount());
                if (bundle.isComplete()) {
                    completeTransaction(marker.getId());
                }
            } else {
                // END arrived before events (rare race condition)
                TransactionBundle newBundle = new TransactionBundle(marker.getId());
                newBundle.markComplete(marker);
                pendingTx.put(marker.getId(), newBundle);
            }
        }
    }

    private void completeTransaction(String txId) {
        TransactionBundle bundle = pendingTx.remove(txId);
        if (bundle != null && bundle.isComplete()) {
            try {
                sinkService.applyTransaction(bundle);
                metrics.counter("cdc.transactions.completed").increment();
                log.info("Transaction {} completed with {} events", txId, bundle.getEvents().size());
            } catch (Exception e) {
                metrics.counter("cdc.transactions.failed").increment();
                log.error("Transaction {} failed: {}", txId, e.getMessage(), e);
                handleFailedTransaction(bundle, e);
            }
        }
    }

    public void cleanupStaleTransactions(Duration timeout) {
        pendingTx.entrySet().removeIf(entry -> {
            TransactionBundle bundle = entry.getValue();
            if (bundle.isTimedOut(timeout)) {
                metrics.counter("cdc.transactions.timeout").increment();
                log.warn("Transaction {} timed out after {}", entry.getKey(), timeout);
                handleTimedOutTransaction(bundle);
                return true;
            }
            return false;
        });
    }

    private void handleFailedTransaction(TransactionBundle bundle, Exception e) {
        // Send to dead-letter topic for manual review
        log.error("Sending failed transaction {} to DLT", bundle.getTxId());
    }

    private void handleTimedOutTransaction(TransactionBundle bundle) {
        // Send to dead-letter topic for manual review
        log.warn("Sending timed-out transaction {} to DLT", bundle.getTxId());
    }

    public int getPendingTransactionCount() {
        return pendingTx.size();
    }
}
```

#### PostgresSinkService.java

```java
package com.o2kafka.aggregator.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.o2kafka.aggregator.model.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Service
public class PostgresSinkService {

    private final JdbcTemplate jdbcTemplate;
    private final Map<String, TableMapping> tableMappings;

    public PostgresSinkService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.tableMappings = initTableMappings();
    }

    @Transactional
    public void applyTransaction(TransactionBundle bundle) {
        // Record transaction for idempotency
        recordTransaction(bundle);

        // Apply each change in order
        for (ChangeEvent event : bundle.getEvents()) {
            applyChange(event);
        }
    }

    private void recordTransaction(TransactionBundle bundle) {
        jdbcTemplate.update(
            "INSERT INTO ecom.cdc_transactions (tx_id, event_count, applied_at) " +
            "VALUES (?, ?, CURRENT_TIMESTAMP) ON CONFLICT (tx_id) DO NOTHING",
            bundle.getTxId(),
            bundle.getEvents().size()
        );
    }

    private void applyChange(ChangeEvent event) {
        TableMapping mapping = tableMappings.get(event.getTable().toLowerCase());
        if (mapping == null) {
            throw new IllegalStateException("No mapping for table: " + event.getTable());
        }

        switch (event.getOperation()) {
            case "c" -> executeInsert(mapping, event.getAfter());
            case "u" -> executeUpdate(mapping, event.getBefore(), event.getAfter());
            case "d" -> executeDelete(mapping, event.getBefore());
            case "r" -> executeUpsert(mapping, event.getAfter()); // snapshot read
            default -> throw new IllegalArgumentException("Unknown operation: " + event.getOperation());
        }
    }

    private void executeInsert(TableMapping mapping, JsonNode after) {
        String sql = mapping.getInsertSql();
        Object[] params = mapping.extractInsertParams(after);
        jdbcTemplate.update(sql, params);
    }

    private void executeUpdate(TableMapping mapping, JsonNode before, JsonNode after) {
        String sql = mapping.getUpdateSql();
        Object[] params = mapping.extractUpdateParams(before, after);
        jdbcTemplate.update(sql, params);
    }

    private void executeDelete(TableMapping mapping, JsonNode before) {
        String sql = mapping.getDeleteSql();
        Object[] params = mapping.extractKeyParams(before);
        jdbcTemplate.update(sql, params);
    }

    private void executeUpsert(TableMapping mapping, JsonNode after) {
        String sql = mapping.getUpsertSql();
        Object[] params = mapping.extractInsertParams(after);
        jdbcTemplate.update(sql, params);
    }

    private Map<String, TableMapping> initTableMappings() {
        // Initialize mappings for all 10 tables
        return Map.of(
            "categories", new TableMapping("ecom.categories", "category_id"),
            "products", new TableMapping("ecom.products", "product_id"),
            "inventory", new TableMapping("ecom.inventory", "inventory_id"),
            "customers", new TableMapping("ecom.customers", "customer_id"),
            "orders", new TableMapping("ecom.orders", "order_id"),
            "order_items", new TableMapping("ecom.order_items", "item_id"),
            "payments", new TableMapping("ecom.payments", "payment_id"),
            "shipments", new TableMapping("ecom.shipments", "shipment_id"),
            "shipment_items", new TableMapping("ecom.shipment_items", "shipment_item_id"),
            "returns", new TableMapping("ecom.returns", "return_id")
        );
    }
}
```

### 5.3 Application Configuration

```yaml
# application.yml
spring:
  application:
    name: tx-aggregator

  datasource:
    url: jdbc:postgresql://postgres:5432/ecom
    username: postgres
    password: postgres
    hikari:
      maximum-pool-size: 20
      minimum-idle: 5

  kafka:
    bootstrap-servers: kafka:9092
    consumer:
      group-id: tx-aggregator
      auto-offset-reset: earliest
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

aggregator:
  transaction:
    timeout-minutes: 5
    cleanup-interval-seconds: 60
  topics:
    cdc-pattern: "o2k\\.ECOM\\..*"
    transaction: "o2k.transaction"

management:
  endpoints:
    web:
      exposure:
        include: health,metrics,prometheus
  metrics:
    export:
      prometheus:
        enabled: true
```

---

## Phase 6: PostgreSQL Target Database

### 6.1 Schema (Mirror of Oracle)

```sql
-- PostgreSQL schema matching Oracle structure
CREATE SCHEMA IF NOT EXISTS ecom;

CREATE TABLE ecom.categories (
    category_id    INTEGER PRIMARY KEY,
    name           VARCHAR(100) NOT NULL,
    parent_id      INTEGER REFERENCES ecom.categories(category_id),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ecom.products (
    product_id     INTEGER PRIMARY KEY,
    sku            VARCHAR(50) UNIQUE NOT NULL,
    name           VARCHAR(200) NOT NULL,
    description    TEXT,
    price          DECIMAL(10,2) NOT NULL,
    category_id    INTEGER REFERENCES ecom.categories(category_id),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ecom.inventory (
    inventory_id   INTEGER PRIMARY KEY,
    product_id     INTEGER REFERENCES ecom.products(product_id),
    warehouse_code VARCHAR(10) NOT NULL,
    quantity       INTEGER NOT NULL,
    reserved       INTEGER DEFAULT 0,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(product_id, warehouse_code)
);

CREATE TABLE ecom.customers (
    customer_id    INTEGER PRIMARY KEY,
    email          VARCHAR(255) UNIQUE NOT NULL,
    first_name     VARCHAR(100),
    last_name      VARCHAR(100),
    phone          VARCHAR(20),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ecom.orders (
    order_id       INTEGER PRIMARY KEY,
    customer_id    INTEGER REFERENCES ecom.customers(customer_id),
    status         VARCHAR(20) DEFAULT 'PENDING',
    total_amount   DECIMAL(12,2),
    shipping_addr  VARCHAR(500),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ecom.order_items (
    item_id        INTEGER PRIMARY KEY,
    order_id       INTEGER REFERENCES ecom.orders(order_id),
    product_id     INTEGER REFERENCES ecom.products(product_id),
    quantity       SMALLINT NOT NULL,
    unit_price     DECIMAL(10,2) NOT NULL,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ecom.payments (
    payment_id     INTEGER PRIMARY KEY,
    order_id       INTEGER REFERENCES ecom.orders(order_id),
    amount         DECIMAL(12,2) NOT NULL,
    method         VARCHAR(20) NOT NULL,
    status         VARCHAR(20) DEFAULT 'PENDING',
    transaction_ref VARCHAR(100),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ecom.shipments (
    shipment_id    INTEGER PRIMARY KEY,
    order_id       INTEGER REFERENCES ecom.orders(order_id),
    carrier        VARCHAR(50),
    tracking_num   VARCHAR(100),
    status         VARCHAR(20) DEFAULT 'PREPARING',
    shipped_at     TIMESTAMP,
    delivered_at   TIMESTAMP,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ecom.shipment_items (
    shipment_item_id INTEGER PRIMARY KEY,
    shipment_id      INTEGER REFERENCES ecom.shipments(shipment_id),
    order_item_id    INTEGER REFERENCES ecom.order_items(item_id),
    quantity         SMALLINT NOT NULL,
    created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE ecom.returns (
    return_id      INTEGER PRIMARY KEY,
    order_id       INTEGER REFERENCES ecom.orders(order_id),
    order_item_id  INTEGER REFERENCES ecom.order_items(item_id),
    quantity       SMALLINT NOT NULL,
    reason         VARCHAR(500),
    status         VARCHAR(20) DEFAULT 'REQUESTED',
    refund_amount  DECIMAL(10,2),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_orders_customer ON ecom.orders(customer_id);
CREATE INDEX idx_order_items_order ON ecom.order_items(order_id);
CREATE INDEX idx_payments_order ON ecom.payments(order_id);
CREATE INDEX idx_shipments_order ON ecom.shipments(order_id);
CREATE INDEX idx_inventory_product ON ecom.inventory(product_id);
```

### 6.2 Audit/Tracking Table

```sql
-- Track applied transactions for idempotency
CREATE TABLE ecom.cdc_transactions (
    tx_id          VARCHAR(50) PRIMARY KEY,
    event_count    INTEGER NOT NULL,
    applied_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_scn     BIGINT
);

CREATE INDEX idx_cdc_tx_applied ON ecom.cdc_transactions(applied_at);
```

---

## Phase 7: Test Scenarios

### 7.1 Test Case 1: Simple Order Creation

**Purpose**: Verify single-table insert is captured and replicated.

```sql
-- Oracle
INSERT INTO orders (order_id, customer_id, status, total_amount)
VALUES (1001, 1, 'PENDING', 299.99);
COMMIT;
```

**Expected**:
- 1 CDC event in `o2k.ECOM.ORDERS`
- 1 transaction with BEGIN/END markers
- 1 row in PostgreSQL `ecom.orders`

### 7.2 Test Case 2: Multi-Table Order Transaction

**Purpose**: Verify transaction atomicity across multiple tables.

```sql
-- Oracle: Complete order flow in single transaction
DECLARE
    v_order_id NUMBER := 1002;
BEGIN
    -- Create order
    INSERT INTO orders (order_id, customer_id, status, total_amount, shipping_addr)
    VALUES (v_order_id, 1, 'CONFIRMED', 549.97, '123 Main St');

    -- Add order items
    INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price)
    VALUES (2001, v_order_id, 101, 2, 199.99);

    INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price)
    VALUES (2002, v_order_id, 102, 1, 149.99);

    -- Update inventory
    UPDATE inventory SET quantity = quantity - 2, reserved = reserved + 2
    WHERE product_id = 101 AND warehouse_code = 'WH01';

    UPDATE inventory SET quantity = quantity - 1, reserved = reserved + 1
    WHERE product_id = 102 AND warehouse_code = 'WH01';

    -- Create payment
    INSERT INTO payments (payment_id, order_id, amount, method, status)
    VALUES (3001, v_order_id, 549.97, 'CREDIT_CARD', 'COMPLETED');

    COMMIT;
END;
/
```

**Expected**:
- 6 CDC events total (1 order + 2 items + 2 inventory + 1 payment)
- All events share same `txId`
- Transaction marker shows `event_count: 6`
- PostgreSQL receives all 6 changes in single transaction
- Either all succeed or all fail (atomicity preserved)

### 7.3 Test Case 3: Concurrent Transactions

**Purpose**: Verify isolation between concurrent transactions.

```sql
-- Session 1: Order A
BEGIN
    INSERT INTO orders (order_id, customer_id, status) VALUES (1003, 2, 'PENDING');
    -- Delay before commit
    DBMS_LOCK.SLEEP(2);
    INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price)
    VALUES (2003, 1003, 103, 1, 99.99);
    COMMIT;
END;
/

-- Session 2: Order B (starts during Session 1)
BEGIN
    INSERT INTO orders (order_id, customer_id, status) VALUES (1004, 3, 'PENDING');
    INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price)
    VALUES (2004, 1004, 104, 3, 49.99);
    COMMIT;
END;
/
```

**Expected**:
- Two separate transaction bundles
- Order B may complete before Order A
- Each transaction applied atomically in PostgreSQL
- No interleaving of events between transactions

### 7.4 Test Case 4: Large Transaction

**Purpose**: Verify handling of transactions with many events.

```sql
-- Oracle: Bulk insert 100 order items
BEGIN
    INSERT INTO orders (order_id, customer_id, status, total_amount)
    VALUES (1005, 1, 'BULK', 9999.99);

    FOR i IN 1..100 LOOP
        INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price)
        VALUES (3000 + i, 1005, MOD(i, 10) + 101, 1, 99.99);
    END LOOP;

    COMMIT;
END;
/
```

**Expected**:
- 101 CDC events in single transaction
- Aggregator buffers all events until END marker
- Single PostgreSQL transaction with 101 inserts
- Verify `total_order` maintains correct sequence

### 7.5 Test Case 5: Rollback Scenario

**Purpose**: Verify rolled-back transactions are not replicated.

```sql
-- Oracle
BEGIN
    INSERT INTO orders (order_id, customer_id, status) VALUES (1006, 1, 'TEST');
    INSERT INTO order_items (item_id, order_id, product_id, quantity, unit_price)
    VALUES (4001, 1006, 101, 1, 99.99);
    ROLLBACK;  -- Rollback instead of commit
END;
/
```

**Expected**:
- No CDC events captured (LogMiner only captures committed changes)
- No transaction markers
- No changes in PostgreSQL

### 7.6 Test Case 6: Transaction Timeout

**Purpose**: Verify handling of incomplete transactions.

**Expected**:
- Aggregator buffers events
- After timeout (5 min), transaction marked as stale
- Events sent to dead-letter topic
- Metrics show `cdc.transactions.timeout` incremented

---

## Phase 8: Verification & Metrics

### 8.1 Data Consistency Checks

```sql
-- Compare row counts between Oracle and PostgreSQL
-- Run against PostgreSQL
SELECT
    'categories' as table_name,
    (SELECT COUNT(*) FROM ecom.categories) as pg_count
UNION ALL
SELECT 'products', (SELECT COUNT(*) FROM ecom.products)
UNION ALL
SELECT 'inventory', (SELECT COUNT(*) FROM ecom.inventory)
UNION ALL
SELECT 'customers', (SELECT COUNT(*) FROM ecom.customers)
UNION ALL
SELECT 'orders', (SELECT COUNT(*) FROM ecom.orders)
UNION ALL
SELECT 'order_items', (SELECT COUNT(*) FROM ecom.order_items)
UNION ALL
SELECT 'payments', (SELECT COUNT(*) FROM ecom.payments)
UNION ALL
SELECT 'shipments', (SELECT COUNT(*) FROM ecom.shipments)
UNION ALL
SELECT 'shipment_items', (SELECT COUNT(*) FROM ecom.shipment_items)
UNION ALL
SELECT 'returns', (SELECT COUNT(*) FROM ecom.returns);
```

### 8.2 Key Metrics to Monitor

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `cdc.events.received` | CDC events consumed | - |
| `cdc.transactions.completed` | Successfully applied transactions | - |
| `cdc.transactions.failed` | Failed transaction applications | > 0 |
| `cdc.transactions.timeout` | Timed-out transactions | > 0 |
| `cdc.lag.seconds` | Time between Oracle commit and PG apply | > 60s |
| `pending.transactions.count` | Currently buffered transactions | > 100 |

### 8.3 Grafana Dashboard Panels

1. **Throughput**: Events/sec, Transactions/sec
2. **Latency**: End-to-end replication lag
3. **Health**: Pending transactions, failures, timeouts
4. **Per-table**: Event counts by table

---

## Phase 9: Project File Structure

```
o2kafka/
├── docker-compose.yml
├── docker-compose.override.yml      # Local dev overrides
├── .env                              # Environment variables
├── CLAUDE.md
├── IMPLEMENTATION_PLAN.md
├── README.md
│
├── docker/
│   ├── oracle/
│   │   ├── Dockerfile
│   │   └── init/
│   │       ├── 01-create-user.sql
│   │       ├── 02-create-schema.sql
│   │       ├── 03-enable-logminer.sql
│   │       └── 04-seed-data.sql
│   ├── postgres/
│   │   └── init/
│   │       └── 01-create-schema.sql
│   └── kafka-connect/
│       └── Dockerfile               # Custom image with Oracle drivers
│
├── connectors/
│   └── oracle-ecom-connector.json
│
├── scripts/
│   ├── create-topics.sh
│   ├── deploy-connector.sh
│   ├── run-tests.sh
│   └── verify-replication.sh
│
├── tx-aggregator/
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/
│       ├── main/
│       │   ├── java/com/o2kafka/aggregator/
│       │   └── resources/
│       └── test/
│
└── tests/
    ├── sql/
    │   ├── test-01-simple-insert.sql
    │   ├── test-02-multi-table-tx.sql
    │   ├── test-03-concurrent-tx.sql
    │   ├── test-04-large-tx.sql
    │   └── test-05-rollback.sql
    └── verification/
        └── compare-data.sql
```

---

## Phase 10: Implementation Sequence

### Step 1: Infrastructure
- [ ] Create Docker Compose with Oracle, Kafka, PostgreSQL
- [ ] Configure Oracle for LogMiner
- [ ] Set up Kafka topics
- [ ] Deploy Kafka Connect with Debezium

### Step 2: Source Schema
- [ ] Create Oracle ECOM schema
- [ ] Enable supplemental logging
- [ ] Seed reference data (categories, products)

### Step 3: Debezium Connector
- [ ] Configure Oracle connector with CTE feature
- [ ] Deploy and validate initial snapshot
- [ ] Verify transaction metadata in events

### Step 4: Target Schema
- [ ] Create PostgreSQL mirror schema
- [ ] Set up CDC tracking table

### Step 5: Transaction Aggregator
- [ ] Implement core aggregation logic
- [ ] Add PostgreSQL sink
- [ ] Implement timeout handling
- [ ] Add metrics and monitoring

### Step 6: Testing
- [ ] Execute test scenarios
- [ ] Verify atomicity preservation
- [ ] Load testing
- [ ] Document results

### Step 7: Documentation
- [ ] Update README
- [ ] Create runbook
- [ ] Document CTE query feature benefits

---

## Success Criteria

1. **Atomicity**: Multi-table Oracle transactions are applied atomically in PostgreSQL
2. **Ordering**: Events within a transaction maintain their original order
3. **Completeness**: All committed Oracle changes appear in PostgreSQL
4. **Consistency**: No partial transactions visible in PostgreSQL
5. **Performance**: Replication lag < 10 seconds under normal load
6. **Reliability**: Zero data loss with proper error handling

---

## References

- [Debezium Oracle Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/oracle.html)
- [Debezium CTE Query Feature Blog Post](https://debezium.io/blog/2025/08/14/oracle-new-feature-cte-query/)
- [Debezium Transaction Metadata](https://debezium.io/documentation/reference/stable/connectors/oracle.html#oracle-transaction-metadata)
- [Kafka Streams Documentation](https://kafka.apache.org/documentation/streams/)
