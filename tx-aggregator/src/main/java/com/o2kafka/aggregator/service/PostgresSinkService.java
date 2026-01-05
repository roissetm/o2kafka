package com.o2kafka.aggregator.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.o2kafka.aggregator.model.ChangeEvent;
import com.o2kafka.aggregator.model.TransactionBundle;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.*;

/**
 * Service responsible for applying CDC events to PostgreSQL.
 * Applies complete transactions atomically within a single database transaction.
 */
@Service
public class PostgresSinkService {

    private static final Logger log = LoggerFactory.getLogger(PostgresSinkService.class);

    private final JdbcTemplate jdbcTemplate;
    private final String targetSchema;
    private final Map<String, TableMapping> tableMappings;

    // Metrics
    private final Counter rowsInsertedCounter;
    private final Counter rowsUpdatedCounter;
    private final Counter rowsDeletedCounter;

    public PostgresSinkService(
            JdbcTemplate jdbcTemplate,
            MeterRegistry meterRegistry,
            @Value("${aggregator.schema.target:ecom}") String targetSchema) {

        this.jdbcTemplate = jdbcTemplate;
        this.targetSchema = targetSchema;
        this.tableMappings = initializeTableMappings();

        // Initialize metrics
        this.rowsInsertedCounter = Counter.builder("cdc.rows.inserted")
                .description("Rows inserted into PostgreSQL")
                .register(meterRegistry);

        this.rowsUpdatedCounter = Counter.builder("cdc.rows.updated")
                .description("Rows updated in PostgreSQL")
                .register(meterRegistry);

        this.rowsDeletedCounter = Counter.builder("cdc.rows.deleted")
                .description("Rows deleted from PostgreSQL")
                .register(meterRegistry);

        log.info("PostgresSinkService initialized with target schema: {}", targetSchema);
    }

    /**
     * Apply a complete transaction atomically.
     * All events are applied within a single database transaction.
     */
    @Transactional
    public void applyTransaction(TransactionBundle bundle) {
        // Record transaction for idempotency checking
        if (isTransactionAlreadyApplied(bundle.getTxId())) {
            log.warn("Transaction {} already applied, skipping", bundle.getTxId());
            return;
        }

        // Apply each event in order
        List<ChangeEvent> events = bundle.getOrderedEvents();
        for (ChangeEvent event : events) {
            applyEvent(event);
        }

        // Record successful transaction
        recordTransaction(bundle);

        log.debug("Applied transaction {} with {} events", bundle.getTxId(), events.size());
    }

    /**
     * Apply a single event (used for snapshots or individual events).
     */
    @Transactional
    public void applySingleEvent(ChangeEvent event) {
        applyEvent(event);
    }

    /**
     * Check if a transaction was already applied (idempotency).
     */
    private boolean isTransactionAlreadyApplied(String txId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM " + targetSchema + ".cdc_transactions WHERE tx_id = ?",
                Integer.class,
                txId
        );
        return count != null && count > 0;
    }

    /**
     * Record a completed transaction.
     */
    private void recordTransaction(TransactionBundle bundle) {
        jdbcTemplate.update(
                "INSERT INTO " + targetSchema + ".cdc_transactions (tx_id, event_count, applied_at) " +
                "VALUES (?, ?, CURRENT_TIMESTAMP) ON CONFLICT (tx_id) DO NOTHING",
                bundle.getTxId(),
                bundle.getEventCount()
        );
    }

    /**
     * Apply a single CDC event to PostgreSQL.
     */
    private void applyEvent(ChangeEvent event) {
        String tableName = event.getTable().toLowerCase();
        TableMapping mapping = tableMappings.get(tableName);

        if (mapping == null) {
            log.warn("No mapping found for table: {}", tableName);
            return;
        }

        switch (event.getOperation()) {
            case "c", "r" -> {  // create or read (snapshot)
                executeInsert(mapping, event.getAfter());
                rowsInsertedCounter.increment();
            }
            case "u" -> {
                executeUpdate(mapping, event.getBefore(), event.getAfter());
                rowsUpdatedCounter.increment();
            }
            case "d" -> {
                executeDelete(mapping, event.getBefore());
                rowsDeletedCounter.increment();
            }
            default -> log.warn("Unknown operation: {} for event: {}", event.getOperation(), event);
        }
    }

    /**
     * Execute an INSERT statement.
     */
    private void executeInsert(TableMapping mapping, JsonNode after) {
        if (after == null || after.isNull() || after.isMissingNode()) {
            log.warn("No 'after' data for insert on table: {}", mapping.tableName);
            return;
        }

        String sql = mapping.buildInsertSql();
        Object[] params = mapping.extractValues(after);

        log.trace("Executing INSERT: {} with {} params", sql, params.length);
        jdbcTemplate.update(sql, params);
    }

    /**
     * Execute an UPDATE statement.
     */
    private void executeUpdate(TableMapping mapping, JsonNode before, JsonNode after) {
        if (after == null || after.isNull() || after.isMissingNode()) {
            log.warn("No 'after' data for update on table: {}", mapping.tableName);
            return;
        }

        String sql = mapping.buildUpdateSql();
        Object[] params = mapping.extractUpdateParams(before, after);

        log.trace("Executing UPDATE: {} with {} params", sql, params.length);
        jdbcTemplate.update(sql, params);
    }

    /**
     * Execute a DELETE statement.
     */
    private void executeDelete(TableMapping mapping, JsonNode before) {
        if (before == null || before.isNull() || before.isMissingNode()) {
            log.warn("No 'before' data for delete on table: {}", mapping.tableName);
            return;
        }

        String sql = mapping.buildDeleteSql();
        Object[] params = mapping.extractKeyValues(before);

        log.trace("Executing DELETE: {} with {} params", sql, params.length);
        jdbcTemplate.update(sql, params);
    }

    /**
     * Initialize table mappings for all 10 ECOM tables.
     */
    private Map<String, TableMapping> initializeTableMappings() {
        Map<String, TableMapping> mappings = new HashMap<>();

        // CATEGORIES
        mappings.put("categories", new TableMapping(
                targetSchema + ".categories",
                List.of("category_id"),
                List.of("category_id", "name", "parent_id", "created_at", "updated_at"),
                Map.of("category_id", "CATEGORY_ID", "name", "NAME", "parent_id", "PARENT_ID",
                       "created_at", "CREATED_AT", "updated_at", "UPDATED_AT")
        ));

        // PRODUCTS
        mappings.put("products", new TableMapping(
                targetSchema + ".products",
                List.of("product_id"),
                List.of("product_id", "sku", "name", "description", "price", "category_id", "created_at", "updated_at"),
                Map.of("product_id", "PRODUCT_ID", "sku", "SKU", "name", "NAME", "description", "DESCRIPTION",
                       "price", "PRICE", "category_id", "CATEGORY_ID", "created_at", "CREATED_AT", "updated_at", "UPDATED_AT")
        ));

        // INVENTORY
        mappings.put("inventory", new TableMapping(
                targetSchema + ".inventory",
                List.of("inventory_id"),
                List.of("inventory_id", "product_id", "warehouse_code", "quantity", "reserved", "updated_at"),
                Map.of("inventory_id", "INVENTORY_ID", "product_id", "PRODUCT_ID", "warehouse_code", "WAREHOUSE_CODE",
                       "quantity", "QUANTITY", "reserved", "RESERVED", "updated_at", "UPDATED_AT")
        ));

        // CUSTOMERS
        mappings.put("customers", new TableMapping(
                targetSchema + ".customers",
                List.of("customer_id"),
                List.of("customer_id", "email", "first_name", "last_name", "phone", "created_at", "updated_at"),
                Map.of("customer_id", "CUSTOMER_ID", "email", "EMAIL", "first_name", "FIRST_NAME",
                       "last_name", "LAST_NAME", "phone", "PHONE", "created_at", "CREATED_AT", "updated_at", "UPDATED_AT")
        ));

        // ORDERS
        mappings.put("orders", new TableMapping(
                targetSchema + ".orders",
                List.of("order_id"),
                List.of("order_id", "customer_id", "status", "total_amount", "shipping_addr", "created_at", "updated_at"),
                Map.of("order_id", "ORDER_ID", "customer_id", "CUSTOMER_ID", "status", "STATUS",
                       "total_amount", "TOTAL_AMOUNT", "shipping_addr", "SHIPPING_ADDR",
                       "created_at", "CREATED_AT", "updated_at", "UPDATED_AT")
        ));

        // ORDER_ITEMS
        mappings.put("order_items", new TableMapping(
                targetSchema + ".order_items",
                List.of("item_id"),
                List.of("item_id", "order_id", "product_id", "quantity", "unit_price", "created_at"),
                Map.of("item_id", "ITEM_ID", "order_id", "ORDER_ID", "product_id", "PRODUCT_ID",
                       "quantity", "QUANTITY", "unit_price", "UNIT_PRICE", "created_at", "CREATED_AT")
        ));

        // PAYMENTS
        mappings.put("payments", new TableMapping(
                targetSchema + ".payments",
                List.of("payment_id"),
                List.of("payment_id", "order_id", "amount", "method", "status", "transaction_ref", "created_at", "updated_at"),
                Map.of("payment_id", "PAYMENT_ID", "order_id", "ORDER_ID", "amount", "AMOUNT",
                       "method", "METHOD", "status", "STATUS", "transaction_ref", "TRANSACTION_REF",
                       "created_at", "CREATED_AT", "updated_at", "UPDATED_AT")
        ));

        // SHIPMENTS
        mappings.put("shipments", new TableMapping(
                targetSchema + ".shipments",
                List.of("shipment_id"),
                List.of("shipment_id", "order_id", "carrier", "tracking_num", "status", "shipped_at", "delivered_at", "created_at", "updated_at"),
                Map.of("shipment_id", "SHIPMENT_ID", "order_id", "ORDER_ID", "carrier", "CARRIER",
                       "tracking_num", "TRACKING_NUM", "status", "STATUS", "shipped_at", "SHIPPED_AT",
                       "delivered_at", "DELIVERED_AT", "created_at", "CREATED_AT", "updated_at", "UPDATED_AT")
        ));

        // SHIPMENT_ITEMS
        mappings.put("shipment_items", new TableMapping(
                targetSchema + ".shipment_items",
                List.of("shipment_item_id"),
                List.of("shipment_item_id", "shipment_id", "order_item_id", "quantity", "created_at"),
                Map.of("shipment_item_id", "SHIPMENT_ITEM_ID", "shipment_id", "SHIPMENT_ID",
                       "order_item_id", "ORDER_ITEM_ID", "quantity", "QUANTITY", "created_at", "CREATED_AT")
        ));

        // RETURNS
        mappings.put("returns", new TableMapping(
                targetSchema + ".returns",
                List.of("return_id"),
                List.of("return_id", "order_id", "order_item_id", "quantity", "reason", "status", "refund_amount", "created_at", "updated_at"),
                Map.of("return_id", "RETURN_ID", "order_id", "ORDER_ID", "order_item_id", "ORDER_ITEM_ID",
                       "quantity", "QUANTITY", "reason", "REASON", "status", "STATUS",
                       "refund_amount", "REFUND_AMOUNT", "created_at", "CREATED_AT", "updated_at", "UPDATED_AT")
        ));

        return mappings;
    }

    /**
     * Inner class representing a table's column mappings and SQL generation.
     */
    private static class TableMapping {
        final String tableName;
        final List<String> primaryKeys;
        final List<String> columns;
        final Map<String, String> sourceColumnMap; // PostgreSQL column -> Oracle column

        TableMapping(String tableName, List<String> primaryKeys, List<String> columns, Map<String, String> sourceColumnMap) {
            this.tableName = tableName;
            this.primaryKeys = primaryKeys;
            this.columns = columns;
            this.sourceColumnMap = sourceColumnMap;
        }

        String buildInsertSql() {
            String cols = String.join(", ", columns);
            String placeholders = String.join(", ", Collections.nCopies(columns.size(), "?"));
            return String.format("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
                    tableName, cols, placeholders,
                    String.join(", ", primaryKeys),
                    buildUpdateSet());
        }

        String buildUpdateSql() {
            List<String> nonKeyColumns = columns.stream()
                    .filter(c -> !primaryKeys.contains(c))
                    .toList();

            String setClause = nonKeyColumns.stream()
                    .map(c -> c + " = ?")
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("");

            String whereClause = primaryKeys.stream()
                    .map(c -> c + " = ?")
                    .reduce((a, b) -> a + " AND " + b)
                    .orElse("1=1");

            return String.format("UPDATE %s SET %s WHERE %s", tableName, setClause, whereClause);
        }

        String buildDeleteSql() {
            String whereClause = primaryKeys.stream()
                    .map(c -> c + " = ?")
                    .reduce((a, b) -> a + " AND " + b)
                    .orElse("1=1");

            return String.format("DELETE FROM %s WHERE %s", tableName, whereClause);
        }

        private String buildUpdateSet() {
            return columns.stream()
                    .filter(c -> !primaryKeys.contains(c))
                    .map(c -> c + " = EXCLUDED." + c)
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("");
        }

        Object[] extractValues(JsonNode data) {
            return columns.stream()
                    .map(col -> extractValue(data, sourceColumnMap.get(col)))
                    .toArray();
        }

        Object[] extractUpdateParams(JsonNode before, JsonNode after) {
            List<Object> params = new ArrayList<>();

            // Non-key columns from after
            for (String col : columns) {
                if (!primaryKeys.contains(col)) {
                    params.add(extractValue(after, sourceColumnMap.get(col)));
                }
            }

            // Primary keys from before (for WHERE clause)
            for (String pk : primaryKeys) {
                params.add(extractValue(before, sourceColumnMap.get(pk)));
            }

            return params.toArray();
        }

        Object[] extractKeyValues(JsonNode data) {
            return primaryKeys.stream()
                    .map(pk -> extractValue(data, sourceColumnMap.get(pk)))
                    .toArray();
        }

        private Object extractValue(JsonNode data, String fieldName) {
            if (data == null || fieldName == null) {
                return null;
            }

            JsonNode value = data.path(fieldName);

            if (value.isNull() || value.isMissingNode()) {
                return null;
            }

            // Handle timestamps FIRST (Debezium sends them as microseconds since epoch)
            // Check by field name pattern before checking numeric type
            if (fieldName.endsWith("_AT") || fieldName.contains("TIME")) {
                if (value.isNumber()) {
                    // Debezium Oracle connector sends timestamps as microseconds
                    long micros = value.asLong();
                    // Convert microseconds to milliseconds for Timestamp
                    return new Timestamp(micros / 1000);
                } else if (value.isTextual()) {
                    // Handle string-formatted timestamps
                    return Timestamp.valueOf(value.asText());
                }
            }

            if (value.isNumber()) {
                // Handle Oracle NUMBER as either int or double
                if (value.isIntegralNumber()) {
                    return value.asLong();
                }
                return value.asDouble();
            }

            if (value.isTextual()) {
                return value.asText();
            }

            if (value.isBoolean()) {
                return value.asBoolean();
            }

            return value.asText();
        }
    }
}
