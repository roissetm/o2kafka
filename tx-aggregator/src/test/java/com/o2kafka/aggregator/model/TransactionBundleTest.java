package com.o2kafka.aggregator.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TransactionBundle model.
 */
@DisplayName("TransactionBundle")
class TransactionBundleTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Nested
    @DisplayName("Event aggregation")
    class EventAggregationTests {

        @Test
        @DisplayName("Should add events and track count")
        void shouldAddEventsAndTrackCount() throws Exception {
            TransactionBundle bundle = new TransactionBundle("tx123");

            assertEquals(0, bundle.getEventCount());

            bundle.addEvent(createEvent("ORDERS", 1));
            assertEquals(1, bundle.getEventCount());

            bundle.addEvent(createEvent("ORDER_ITEMS", 2));
            bundle.addEvent(createEvent("PAYMENTS", 3));
            assertEquals(3, bundle.getEventCount());
        }

        @Test
        @DisplayName("Should return events sorted by total_order")
        void shouldReturnEventsSortedByTotalOrder() throws Exception {
            TransactionBundle bundle = new TransactionBundle("tx123");

            // Add events out of order
            bundle.addEvent(createEvent("PAYMENTS", 5));
            bundle.addEvent(createEvent("ORDERS", 1));
            bundle.addEvent(createEvent("INVENTORY", 3));
            bundle.addEvent(createEvent("ORDER_ITEMS", 2));
            bundle.addEvent(createEvent("INVENTORY", 4));

            List<ChangeEvent> ordered = bundle.getOrderedEvents();

            assertEquals(5, ordered.size());
            assertEquals("ORDERS", ordered.get(0).getTable());
            assertEquals("ORDER_ITEMS", ordered.get(1).getTable());
            assertEquals("INVENTORY", ordered.get(2).getTable());
            assertEquals("INVENTORY", ordered.get(3).getTable());
            assertEquals("PAYMENTS", ordered.get(4).getTable());
        }

        @Test
        @DisplayName("Should track tables involved in transaction")
        void shouldTrackTablesInvolved() throws Exception {
            TransactionBundle bundle = new TransactionBundle("tx123");

            bundle.addEvent(createEvent("ORDERS", 1));
            bundle.addEvent(createEvent("ORDER_ITEMS", 2));
            bundle.addEvent(createEvent("ORDER_ITEMS", 3));
            bundle.addEvent(createEvent("INVENTORY", 4));
            bundle.addEvent(createEvent("INVENTORY", 5));
            bundle.addEvent(createEvent("INVENTORY", 6));
            bundle.addEvent(createEvent("PAYMENTS", 7));

            Map<String, Long> tableCounts = bundle.getEventCountByTable();

            assertEquals(4, tableCounts.size());
            assertEquals(1L, tableCounts.get("ORDERS"));
            assertEquals(2L, tableCounts.get("ORDER_ITEMS"));
            assertEquals(3L, tableCounts.get("INVENTORY"));
            assertEquals(1L, tableCounts.get("PAYMENTS"));
        }
    }

    @Nested
    @DisplayName("Transaction completion")
    class CompletionTests {

        @Test
        @DisplayName("Should track expected event count from end marker")
        void shouldTrackExpectedEventCountFromEndMarker() throws Exception {
            TransactionBundle bundle = new TransactionBundle("tx123");

            assertEquals(-1, bundle.getExpectedCount());
            assertFalse(bundle.isEndMarkerReceived());

            TransactionMarker endMarker = createEndMarker("tx123", 5);
            bundle.markComplete(endMarker);

            assertEquals(5, bundle.getExpectedCount());
            assertTrue(bundle.isEndMarkerReceived());
        }

        @Test
        @DisplayName("Should detect complete transaction")
        void shouldDetectCompleteTransaction() throws Exception {
            TransactionBundle bundle = new TransactionBundle("tx123");

            assertFalse(bundle.isComplete());

            bundle.addEvent(createEvent("ORDERS", 1));
            assertFalse(bundle.isComplete());

            bundle.addEvent(createEvent("ORDER_ITEMS", 2));
            assertFalse(bundle.isComplete());

            bundle.addEvent(createEvent("PAYMENTS", 3));
            assertFalse(bundle.isComplete()); // End marker not received yet

            // Receive end marker
            bundle.markComplete(createEndMarker("tx123", 3));
            assertTrue(bundle.isComplete());
        }

        @Test
        @DisplayName("Should not be complete without end marker")
        void shouldNotBeCompleteWithoutEndMarker() throws Exception {
            TransactionBundle bundle = new TransactionBundle("tx123");

            bundle.addEvent(createEvent("ORDERS", 1));
            bundle.addEvent(createEvent("ORDER_ITEMS", 2));
            bundle.addEvent(createEvent("PAYMENTS", 3));

            // Even with events, not complete without end marker
            assertFalse(bundle.isComplete());
        }

        @Test
        @DisplayName("Should handle empty transaction")
        void shouldHandleEmptyTransaction() throws Exception {
            TransactionBundle bundle = new TransactionBundle("tx123");
            bundle.markComplete(createEndMarker("tx123", 0));

            assertTrue(bundle.isComplete());
            assertEquals(0, bundle.getEventCount());
        }
    }

    @Nested
    @DisplayName("Transaction properties")
    class PropertiesTests {

        @Test
        @DisplayName("Should store transaction ID")
        void shouldStoreTransactionId() {
            TransactionBundle bundle = new TransactionBundle("040011009e020000");
            assertEquals("040011009e020000", bundle.getTxId());
        }

        @Test
        @DisplayName("Should track creation time")
        void shouldTrackCreationTime() throws Exception {
            TransactionBundle bundle = new TransactionBundle("tx123");

            assertNotNull(bundle.getStartTime());
            // Start time should be recent (within last second)
            assertTrue(bundle.getStartTime().toEpochMilli() > System.currentTimeMillis() - 1000);
        }

        @Test
        @DisplayName("Should calculate waiting time correctly")
        void shouldCalculateWaitingTimeCorrectly() throws Exception {
            TransactionBundle bundle = new TransactionBundle("tx123");
            Thread.sleep(50);

            Duration waitingTime = bundle.getWaitingTime();
            assertTrue(waitingTime.toMillis() >= 50);
            assertTrue(waitingTime.toMillis() < 1000); // Sanity check
        }

        @Test
        @DisplayName("Should detect timeout")
        void shouldDetectTimeout() throws Exception {
            TransactionBundle bundle = new TransactionBundle("tx123");

            // Short timeout for testing
            assertFalse(bundle.isTimedOut(Duration.ofSeconds(10)));

            Thread.sleep(60);
            assertTrue(bundle.isTimedOut(Duration.ofMillis(50)));
        }
    }

    @Nested
    @DisplayName("toString representation")
    class ToStringTests {

        @Test
        @DisplayName("Should produce readable toString output")
        void shouldProduceReadableToStringOutput() throws Exception {
            TransactionBundle bundle = new TransactionBundle("tx-test");
            bundle.addEvent(createEvent("ORDERS", 1));
            bundle.addEvent(createEvent("ORDER_ITEMS", 2));

            String str = bundle.toString();
            assertTrue(str.contains("tx-test"));
            assertTrue(str.contains("2")); // event count
        }
    }

    private ChangeEvent createEvent(String table, long totalOrder) throws Exception {
        String json = String.format("""
            {
                "op": "c",
                "after": {"ID": 1},
                "source": {"table": "%s"},
                "transaction": {
                    "id": "tx123",
                    "total_order": %d,
                    "data_collection_order": 1
                }
            }
            """, table, totalOrder);

        JsonNode payload = objectMapper.readTree(json);
        return new ChangeEvent("o2k.ECOM." + table, payload);
    }

    private TransactionMarker createEndMarker(String txId, int eventCount) throws Exception {
        String json = String.format("""
            {
                "status": "END",
                "id": "%s",
                "event_count": %d
            }
            """, txId, eventCount);

        JsonNode payload = objectMapper.readTree(json);
        return new TransactionMarker(payload);
    }
}
