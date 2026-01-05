package com.o2kafka.aggregator.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ChangeEvent model.
 */
@DisplayName("ChangeEvent")
class ChangeEventTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Nested
    @DisplayName("Construction and parsing")
    class ConstructionTests {

        @Test
        @DisplayName("Should parse INSERT event with transaction metadata")
        void shouldParseInsertEventWithTransactionMetadata() throws Exception {
            String json = """
                {
                    "op": "c",
                    "before": null,
                    "after": {
                        "ORDER_ID": 1001,
                        "CUSTOMER_ID": 1,
                        "STATUS": "PENDING",
                        "TOTAL_AMOUNT": 599.97
                    },
                    "source": {
                        "table": "ORDERS",
                        "txId": "040011009e020000",
                        "scn": "12345678"
                    },
                    "transaction": {
                        "id": "040011009e020000",
                        "total_order": 1,
                        "data_collection_order": 1
                    }
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            ChangeEvent event = new ChangeEvent("o2k.ECOM.ORDERS", payload);

            assertEquals("ORDERS", event.getTable());
            assertEquals("c", event.getOperation());
            assertEquals("040011009e020000", event.getTxId());
            assertEquals(1, event.getTotalOrder());
            assertNotNull(event.getAfter());
            assertFalse(event.getAfter().isMissingNode());
            // before is NullNode, not Java null
            assertTrue(event.getBefore().isNull() || event.getBefore().isMissingNode());
        }

        @Test
        @DisplayName("Should parse UPDATE event with before and after")
        void shouldParseUpdateEventWithBeforeAndAfter() throws Exception {
            String json = """
                {
                    "op": "u",
                    "before": {
                        "INVENTORY_ID": 1,
                        "PRODUCT_ID": 101,
                        "QUANTITY": 100
                    },
                    "after": {
                        "INVENTORY_ID": 1,
                        "PRODUCT_ID": 101,
                        "QUANTITY": 99
                    },
                    "source": {
                        "table": "INVENTORY"
                    },
                    "transaction": {
                        "id": "040011009e020000",
                        "total_order": 2,
                        "data_collection_order": 1
                    }
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            ChangeEvent event = new ChangeEvent("o2k.ECOM.INVENTORY", payload);

            assertEquals("INVENTORY", event.getTable());
            assertEquals("u", event.getOperation());
            assertFalse(event.getBefore().isNull());
            assertFalse(event.getAfter().isNull());
            assertEquals(100, event.getBefore().get("QUANTITY").asInt());
            assertEquals(99, event.getAfter().get("QUANTITY").asInt());
        }

        @Test
        @DisplayName("Should parse DELETE event with before only")
        void shouldParseDeleteEventWithBeforeOnly() throws Exception {
            String json = """
                {
                    "op": "d",
                    "before": {
                        "ORDER_ID": 1001,
                        "CUSTOMER_ID": 1
                    },
                    "after": null,
                    "source": {
                        "table": "ORDERS"
                    },
                    "transaction": {
                        "id": "040011009e020000",
                        "total_order": 3,
                        "data_collection_order": 1
                    }
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            ChangeEvent event = new ChangeEvent("o2k.ECOM.ORDERS", payload);

            assertEquals("d", event.getOperation());
            assertFalse(event.getBefore().isNull());
            // after is NullNode, not Java null
            assertTrue(event.getAfter().isNull() || event.getAfter().isMissingNode());
        }

        @Test
        @DisplayName("Should parse snapshot event without transaction metadata")
        void shouldParseSnapshotEventWithoutTransactionMetadata() throws Exception {
            String json = """
                {
                    "op": "r",
                    "before": null,
                    "after": {
                        "PRODUCT_ID": 101,
                        "NAME": "Laptop",
                        "PRICE": 999.99
                    },
                    "source": {
                        "table": "PRODUCTS",
                        "snapshot": "true"
                    }
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            ChangeEvent event = new ChangeEvent("o2k.ECOM.PRODUCTS", payload);

            assertEquals("PRODUCTS", event.getTable());
            assertEquals("r", event.getOperation());
            // txId will be empty string from asText(null) on missing node
            assertTrue(event.getTxId() == null || event.getTxId().isEmpty());
            assertEquals(0, event.getTotalOrder());
        }

        @Test
        @DisplayName("Should extract table name from topic")
        void shouldExtractTableNameFromTopic() throws Exception {
            String json = """
                {
                    "op": "c",
                    "after": {"ID": 1},
                    "source": {"table": "ORDER_ITEMS"}
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            ChangeEvent event = new ChangeEvent("o2k.ECOM.ORDER_ITEMS", payload);

            assertEquals("ORDER_ITEMS", event.getTable());
        }
    }

    @Nested
    @DisplayName("Transaction ordering")
    class OrderingTests {

        @Test
        @DisplayName("Should compare events by total_order")
        void shouldCompareEventsByTotalOrder() throws Exception {
            ChangeEvent event1 = createEventWithOrder(1);
            ChangeEvent event2 = createEventWithOrder(2);
            ChangeEvent event3 = createEventWithOrder(3);

            assertTrue(event1.compareTo(event2) < 0);
            assertTrue(event2.compareTo(event3) < 0);
            assertTrue(event3.compareTo(event1) > 0);
            assertEquals(0, event1.compareTo(event1));
        }

        private ChangeEvent createEventWithOrder(int totalOrder) throws Exception {
            String json = String.format("""
                {
                    "op": "c",
                    "after": {"ID": 1},
                    "source": {"table": "TEST"},
                    "transaction": {
                        "id": "tx1",
                        "total_order": %d,
                        "data_collection_order": 1
                    }
                }
                """, totalOrder);

            JsonNode payload = objectMapper.readTree(json);
            return new ChangeEvent("o2k.ECOM.TEST", payload);
        }
    }

    @Nested
    @DisplayName("Operation type checks")
    class OperationTypeTests {

        @Test
        @DisplayName("Should identify snapshot event")
        void shouldIdentifySnapshotEvent() throws Exception {
            ChangeEvent event = createEventWithOp("r");
            assertTrue(event.isSnapshot());
            assertFalse(event.isInsert());
            assertFalse(event.isUpdate());
            assertFalse(event.isDelete());
        }

        @Test
        @DisplayName("Should identify insert event")
        void shouldIdentifyInsertEvent() throws Exception {
            ChangeEvent event = createEventWithOp("c");
            assertTrue(event.isInsert());
            assertFalse(event.isSnapshot());
            assertFalse(event.isUpdate());
            assertFalse(event.isDelete());
        }

        @Test
        @DisplayName("Should identify update event")
        void shouldIdentifyUpdateEvent() throws Exception {
            ChangeEvent event = createEventWithOp("u");
            assertTrue(event.isUpdate());
            assertFalse(event.isSnapshot());
            assertFalse(event.isInsert());
            assertFalse(event.isDelete());
        }

        @Test
        @DisplayName("Should identify delete event")
        void shouldIdentifyDeleteEvent() throws Exception {
            ChangeEvent event = createEventWithOp("d");
            assertTrue(event.isDelete());
            assertFalse(event.isSnapshot());
            assertFalse(event.isInsert());
            assertFalse(event.isUpdate());
        }

        private ChangeEvent createEventWithOp(String op) throws Exception {
            String json = String.format("""
                {
                    "op": "%s",
                    "after": {"ID": 1},
                    "source": {"table": "TEST"}
                }
                """, op);

            JsonNode payload = objectMapper.readTree(json);
            return new ChangeEvent("o2k.ECOM.TEST", payload);
        }
    }

    @Nested
    @DisplayName("Edge cases")
    class EdgeCaseTests {

        @Test
        @DisplayName("Should handle missing transaction block")
        void shouldHandleMissingTransactionBlock() throws Exception {
            String json = """
                {
                    "op": "c",
                    "after": {"ID": 1},
                    "source": {"table": "TEST"}
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            ChangeEvent event = new ChangeEvent("o2k.ECOM.TEST", payload);

            assertTrue(event.getTxId() == null || event.getTxId().isEmpty());
            assertEquals(0, event.getTotalOrder());
        }

        @Test
        @DisplayName("Should handle topic without schema prefix")
        void shouldHandleTopicWithoutSchemaPrefix() throws Exception {
            String json = """
                {
                    "op": "c",
                    "after": {"ID": 1},
                    "source": {"table": "SIMPLE"}
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            ChangeEvent event = new ChangeEvent("SIMPLE", payload);

            assertEquals("SIMPLE", event.getTable());
        }
    }
}
