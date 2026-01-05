package com.o2kafka.aggregator.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.o2kafka.aggregator.model.ChangeEvent;
import com.o2kafka.aggregator.model.TransactionBundle;
import com.o2kafka.aggregator.model.TransactionMarker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for PostgresSinkService.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("PostgresSinkService")
class PostgresSinkServiceTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private PostgresSinkService sinkService;
    private ObjectMapper objectMapper;

    @Captor
    private ArgumentCaptor<String> sqlCaptor;

    @Captor
    private ArgumentCaptor<Object[]> paramsCaptor;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        sinkService = new PostgresSinkService(
                jdbcTemplate,
                new SimpleMeterRegistry(),
                "ecom"
        );
    }

    @Nested
    @DisplayName("INSERT operations")
    class InsertOperationTests {

        @Test
        @DisplayName("Should generate correct INSERT SQL for orders")
        void shouldGenerateCorrectInsertSqlForOrders() throws Exception {
            ChangeEvent event = createInsertEvent("ORDERS", """
                {
                    "ORDER_ID": 1001,
                    "CUSTOMER_ID": 1,
                    "STATUS": "PENDING",
                    "TOTAL_AMOUNT": 599.97,
                    "SHIPPING_ADDR": "123 Test St",
                    "CREATED_AT": 1704067200000000,
                    "UPDATED_AT": 1704067200000000
                }
                """);

            sinkService.applySingleEvent(event);

            verify(jdbcTemplate).update(sqlCaptor.capture(), any(Object[].class));

            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("INSERT INTO ecom.orders"));
            assertTrue(sql.contains("order_id"));
            assertTrue(sql.contains("customer_id"));
            assertTrue(sql.contains("ON CONFLICT"));
        }

        @Test
        @DisplayName("Should generate correct INSERT SQL for order_items")
        void shouldGenerateCorrectInsertSqlForOrderItems() throws Exception {
            ChangeEvent event = createInsertEvent("ORDER_ITEMS", """
                {
                    "ITEM_ID": 2001,
                    "ORDER_ID": 1001,
                    "PRODUCT_ID": 101,
                    "QUANTITY": 2,
                    "UNIT_PRICE": 149.99,
                    "CREATED_AT": 1704067200000000
                }
                """);

            sinkService.applySingleEvent(event);

            verify(jdbcTemplate).update(sqlCaptor.capture(), any(Object[].class));

            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("INSERT INTO ecom.order_items"));
            assertTrue(sql.contains("item_id"));
            assertTrue(sql.contains("product_id"));
        }
    }

    @Nested
    @DisplayName("UPDATE operations")
    class UpdateOperationTests {

        @Test
        @DisplayName("Should generate correct UPDATE SQL")
        void shouldGenerateCorrectUpdateSql() throws Exception {
            ChangeEvent event = createUpdateEvent("INVENTORY", """
                {"INVENTORY_ID": 1, "PRODUCT_ID": 101, "QUANTITY": 100}
                """, """
                {"INVENTORY_ID": 1, "PRODUCT_ID": 101, "QUANTITY": 98, "UPDATED_AT": 1704067200000000}
                """);

            sinkService.applySingleEvent(event);

            verify(jdbcTemplate).update(sqlCaptor.capture(), any(Object[].class));

            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("UPDATE ecom.inventory"));
            assertTrue(sql.contains("SET"));
            assertTrue(sql.contains("WHERE"));
            assertTrue(sql.contains("inventory_id = ?"));
        }
    }

    @Nested
    @DisplayName("DELETE operations")
    class DeleteOperationTests {

        @Test
        @DisplayName("Should generate correct DELETE SQL")
        void shouldGenerateCorrectDeleteSql() throws Exception {
            ChangeEvent event = createDeleteEvent("ORDERS", """
                {"ORDER_ID": 1001, "CUSTOMER_ID": 1}
                """);

            sinkService.applySingleEvent(event);

            verify(jdbcTemplate).update(sqlCaptor.capture(), any(Object[].class));

            String sql = sqlCaptor.getValue();
            assertTrue(sql.contains("DELETE FROM ecom.orders"));
            assertTrue(sql.contains("WHERE"));
            assertTrue(sql.contains("order_id = ?"));
        }
    }

    @Nested
    @DisplayName("Transaction idempotency")
    class IdempotencyTests {

        @Test
        @DisplayName("Should skip already applied transaction")
        void shouldSkipAlreadyAppliedTransaction() throws Exception {
            TransactionBundle bundle = new TransactionBundle("tx-already-done");
            bundle.addEvent(createInsertEvent("ORDERS", """
                {"ORDER_ID": 1001, "CUSTOMER_ID": 1}
                """));
            bundle.markComplete(createEndMarker("tx-already-done", 1));

            // Transaction already exists
            when(jdbcTemplate.queryForObject(
                    contains("cdc_transactions"),
                    eq(Integer.class),
                    eq("tx-already-done")
            )).thenReturn(1);

            sinkService.applyTransaction(bundle);

            // Should not execute any INSERT/UPDATE/DELETE
            verify(jdbcTemplate, never()).update(argThat(sql ->
                sql.contains("INSERT INTO ecom.orders") ||
                sql.contains("UPDATE ecom.orders") ||
                sql.contains("DELETE FROM ecom.orders")
            ), any(Object[].class));
        }

        @Test
        @DisplayName("Should apply new transaction and record it")
        void shouldApplyNewTransactionAndRecordIt() throws Exception {
            TransactionBundle bundle = new TransactionBundle("tx-new");
            bundle.addEvent(createInsertEvent("ORDERS", """
                {"ORDER_ID": 1001, "CUSTOMER_ID": 1, "STATUS": "PENDING", "TOTAL_AMOUNT": 100.00}
                """));
            bundle.markComplete(createEndMarker("tx-new", 1));

            // Transaction does not exist
            when(jdbcTemplate.queryForObject(
                    contains("cdc_transactions"),
                    eq(Integer.class),
                    eq("tx-new")
            )).thenReturn(0);

            sinkService.applyTransaction(bundle);

            // Should record the transaction
            verify(jdbcTemplate).update(
                    argThat(sql -> sql.contains("INSERT INTO ecom.cdc_transactions")),
                    eq("tx-new"),
                    eq(1)
            );
        }
    }

    @Nested
    @DisplayName("Table mapping")
    class TableMappingTests {

        @Test
        @DisplayName("Should handle all 10 ECOM tables")
        void shouldHandleAllEcomTables() throws Exception {
            String[] tables = {
                "CATEGORIES", "PRODUCTS", "INVENTORY", "CUSTOMERS", "ORDERS",
                "ORDER_ITEMS", "PAYMENTS", "SHIPMENTS", "SHIPMENT_ITEMS", "RETURNS"
            };

            for (String table : tables) {
                ChangeEvent event = createInsertEvent(table, """
                    {"ID": 1}
                    """);

                // Should not throw - table mapping exists
                assertDoesNotThrow(() -> {
                    sinkService.applySingleEvent(event);
                }, "Failed for table: " + table);
            }
        }

        @Test
        @DisplayName("Should warn on unmapped table")
        void shouldWarnOnUnmappedTable() throws Exception {
            ChangeEvent event = createInsertEvent("UNKNOWN_TABLE", """
                {"ID": 1}
                """);

            // Should not throw, just warn
            assertDoesNotThrow(() -> sinkService.applySingleEvent(event));

            // Should not attempt to insert into unknown table
            verify(jdbcTemplate, never()).update(
                    argThat(sql -> sql.contains("unknown_table")),
                    any(Object[].class)
            );
        }
    }

    @Nested
    @DisplayName("Data type conversion")
    class DataTypeConversionTests {

        @Test
        @DisplayName("Should convert Oracle timestamp (microseconds) to SQL Timestamp")
        void shouldConvertOracleTimestampToSqlTimestamp() throws Exception {
            // 1704067200000000 microseconds = 2024-01-01 00:00:00 UTC (in milliseconds: 1704067200000)
            ChangeEvent event = createInsertEvent("ORDERS", """
                {
                    "ORDER_ID": 1,
                    "CUSTOMER_ID": 1,
                    "STATUS": "PENDING",
                    "TOTAL_AMOUNT": 100.00,
                    "CREATED_AT": 1704067200000000,
                    "UPDATED_AT": 1704067200000000
                }
                """);

            sinkService.applySingleEvent(event);

            verify(jdbcTemplate).update(anyString(), paramsCaptor.capture());

            Object[] params = paramsCaptor.getValue();
            // Find timestamp parameters (created_at, updated_at)
            boolean foundTimestamp = false;
            for (Object param : params) {
                if (param instanceof java.sql.Timestamp) {
                    foundTimestamp = true;
                    java.sql.Timestamp ts = (java.sql.Timestamp) param;
                    // Should be around 2024-01-01
                    assertTrue(ts.getTime() > 1700000000000L, "Timestamp too early");
                    assertTrue(ts.getTime() < 1800000000000L, "Timestamp too late");
                }
            }
            assertTrue(foundTimestamp, "Should have timestamp parameters");
        }

        @Test
        @DisplayName("Should handle null values")
        void shouldHandleNullValues() throws Exception {
            ChangeEvent event = createInsertEvent("SHIPMENTS", """
                {
                    "SHIPMENT_ID": 1,
                    "ORDER_ID": 1001,
                    "CARRIER": "UPS",
                    "TRACKING_NUM": null,
                    "STATUS": "PENDING",
                    "SHIPPED_AT": null,
                    "DELIVERED_AT": null,
                    "CREATED_AT": 1704067200000000,
                    "UPDATED_AT": 1704067200000000
                }
                """);

            // Should not throw on null values
            assertDoesNotThrow(() -> sinkService.applySingleEvent(event));
        }

        @Test
        @DisplayName("Should handle numeric types correctly")
        void shouldHandleNumericTypesCorrectly() throws Exception {
            ChangeEvent event = createInsertEvent("PRODUCTS", """
                {
                    "PRODUCT_ID": 101,
                    "SKU": "LAPTOP-001",
                    "NAME": "Laptop",
                    "DESCRIPTION": "A nice laptop",
                    "PRICE": 999.99,
                    "CATEGORY_ID": 1,
                    "CREATED_AT": 1704067200000000,
                    "UPDATED_AT": 1704067200000000
                }
                """);

            sinkService.applySingleEvent(event);

            verify(jdbcTemplate).update(anyString(), paramsCaptor.capture());

            Object[] params = paramsCaptor.getValue();
            boolean foundLong = false;
            boolean foundDouble = false;
            for (Object param : params) {
                if (param instanceof Long) foundLong = true;
                if (param instanceof Double) foundDouble = true;
            }
            assertTrue(foundLong, "Should have Long parameters");
            assertTrue(foundDouble, "Should have Double parameters");
        }
    }

    // Helper methods

    private ChangeEvent createInsertEvent(String table, String afterJson) throws Exception {
        String json = String.format("""
            {
                "op": "c",
                "before": null,
                "after": %s,
                "source": {"table": "%s"}
            }
            """, afterJson, table);
        return new ChangeEvent("o2k.ECOM." + table, objectMapper.readTree(json));
    }

    private ChangeEvent createUpdateEvent(String table, String beforeJson, String afterJson) throws Exception {
        String json = String.format("""
            {
                "op": "u",
                "before": %s,
                "after": %s,
                "source": {"table": "%s"}
            }
            """, beforeJson, afterJson, table);
        return new ChangeEvent("o2k.ECOM." + table, objectMapper.readTree(json));
    }

    private ChangeEvent createDeleteEvent(String table, String beforeJson) throws Exception {
        String json = String.format("""
            {
                "op": "d",
                "before": %s,
                "after": null,
                "source": {"table": "%s"}
            }
            """, beforeJson, table);
        return new ChangeEvent("o2k.ECOM." + table, objectMapper.readTree(json));
    }

    private TransactionMarker createEndMarker(String txId, int eventCount) throws Exception {
        String json = String.format("""
            {
                "status": "END",
                "id": "%s",
                "event_count": %d
            }
            """, txId, eventCount);
        return new TransactionMarker(objectMapper.readTree(json));
    }
}
