package com.o2kafka.aggregator.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TransactionMarker model.
 */
@DisplayName("TransactionMarker")
class TransactionMarkerTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Nested
    @DisplayName("BEGIN marker parsing")
    class BeginMarkerTests {

        @Test
        @DisplayName("Should parse BEGIN marker")
        void shouldParseBeginMarker() throws Exception {
            String json = """
                {
                    "status": "BEGIN",
                    "id": "040011009e020000",
                    "event_count": null,
                    "data_collections": null
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            TransactionMarker marker = new TransactionMarker(payload);

            assertEquals(TransactionMarker.Status.BEGIN, marker.getStatus());
            assertEquals("040011009e020000", marker.getId());
            assertTrue(marker.isBegin());
            assertFalse(marker.isEnd());
            assertEquals(0, marker.getEventCount());
        }

        @Test
        @DisplayName("Should handle BEGIN with zero event count")
        void shouldHandleBeginWithZeroEventCount() throws Exception {
            String json = """
                {
                    "status": "BEGIN",
                    "id": "tx123",
                    "event_count": 0
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            TransactionMarker marker = new TransactionMarker(payload);

            assertTrue(marker.isBegin());
            assertEquals(0, marker.getEventCount());
        }
    }

    @Nested
    @DisplayName("END marker parsing")
    class EndMarkerTests {

        @Test
        @DisplayName("Should parse END marker with event count")
        void shouldParseEndMarkerWithEventCount() throws Exception {
            String json = """
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
                """;

            JsonNode payload = objectMapper.readTree(json);
            TransactionMarker marker = new TransactionMarker(payload);

            assertEquals(TransactionMarker.Status.END, marker.getStatus());
            assertEquals("040011009e020000", marker.getId());
            assertFalse(marker.isBegin());
            assertTrue(marker.isEnd());
            assertEquals(8, marker.getEventCount());
        }

        @Test
        @DisplayName("Should parse END marker with single event")
        void shouldParseEndMarkerWithSingleEvent() throws Exception {
            String json = """
                {
                    "status": "END",
                    "id": "tx456",
                    "event_count": 1,
                    "data_collections": [
                        {"data_collection": "ECOM.CUSTOMERS", "event_count": 1}
                    ]
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            TransactionMarker marker = new TransactionMarker(payload);

            assertTrue(marker.isEnd());
            assertEquals(1, marker.getEventCount());
        }

        @Test
        @DisplayName("Should parse END marker with large event count")
        void shouldParseEndMarkerWithLargeEventCount() throws Exception {
            String json = """
                {
                    "status": "END",
                    "id": "batch-tx",
                    "event_count": 10000
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            TransactionMarker marker = new TransactionMarker(payload);

            assertEquals(10000, marker.getEventCount());
        }
    }

    @Nested
    @DisplayName("Edge cases")
    class EdgeCaseTests {

        @Test
        @DisplayName("Should handle missing event_count field")
        void shouldHandleMissingEventCountField() throws Exception {
            String json = """
                {
                    "status": "END",
                    "id": "tx789"
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            TransactionMarker marker = new TransactionMarker(payload);

            assertEquals(0, marker.getEventCount());
        }

        @Test
        @DisplayName("Should handle null event_count")
        void shouldHandleNullEventCount() throws Exception {
            String json = """
                {
                    "status": "BEGIN",
                    "id": "tx000",
                    "event_count": null
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            TransactionMarker marker = new TransactionMarker(payload);

            assertEquals(0, marker.getEventCount());
        }

        @Test
        @DisplayName("Should handle different transaction ID formats")
        void shouldHandleDifferentTransactionIdFormats() throws Exception {
            // Hex format
            TransactionMarker marker1 = parseMarker("""
                {"status": "BEGIN", "id": "040011009e020000"}
                """);
            assertEquals("040011009e020000", marker1.getId());

            // Numeric format
            TransactionMarker marker2 = parseMarker("""
                {"status": "BEGIN", "id": "123456789"}
                """);
            assertEquals("123456789", marker2.getId());

            // UUID-like format
            TransactionMarker marker3 = parseMarker("""
                {"status": "BEGIN", "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890"}
                """);
            assertEquals("a1b2c3d4-e5f6-7890-abcd-ef1234567890", marker3.getId());
        }
    }

    @Nested
    @DisplayName("Status checking")
    class StatusCheckingTests {

        @Test
        @DisplayName("Should correctly identify BEGIN status")
        void shouldCorrectlyIdentifyBeginStatus() throws Exception {
            TransactionMarker marker = parseMarker("""
                {"status": "BEGIN", "id": "tx1"}
                """);

            assertTrue(marker.isBegin());
            assertFalse(marker.isEnd());
        }

        @Test
        @DisplayName("Should correctly identify END status")
        void shouldCorrectlyIdentifyEndStatus() throws Exception {
            TransactionMarker marker = parseMarker("""
                {"status": "END", "id": "tx1", "event_count": 5}
                """);

            assertFalse(marker.isBegin());
            assertTrue(marker.isEnd());
        }

        @Test
        @DisplayName("Should handle case-insensitive status parsing")
        void shouldHandleCaseInsensitiveStatus() throws Exception {
            // Implementation uses equalsIgnoreCase, so lowercase should work
            TransactionMarker marker1 = parseMarker("""
                {"status": "BEGIN", "id": "tx1"}
                """);
            assertTrue(marker1.isBegin());

            // Lowercase also matches due to equalsIgnoreCase
            TransactionMarker marker2 = parseMarker("""
                {"status": "begin", "id": "tx2"}
                """);
            assertTrue(marker2.isBegin());
        }
    }

    @Nested
    @DisplayName("Data collections parsing")
    class DataCollectionsTests {

        @Test
        @DisplayName("Should parse data collections from END marker")
        void shouldParseDataCollectionsFromEndMarker() throws Exception {
            String json = """
                {
                    "status": "END",
                    "id": "tx123",
                    "event_count": 5,
                    "data_collections": [
                        {"data_collection": "XEPDB1.ECOM.ORDERS", "event_count": 2},
                        {"data_collection": "XEPDB1.ECOM.INVENTORY", "event_count": 3}
                    ]
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            TransactionMarker marker = new TransactionMarker(payload);

            var collections = marker.getDataCollections();
            assertEquals(2, collections.size());
            assertEquals(2, collections.get("ORDERS"));
            assertEquals(3, collections.get("INVENTORY"));
        }

        @Test
        @DisplayName("Should handle empty data collections")
        void shouldHandleEmptyDataCollections() throws Exception {
            String json = """
                {
                    "status": "END",
                    "id": "tx123",
                    "event_count": 0,
                    "data_collections": []
                }
                """;

            JsonNode payload = objectMapper.readTree(json);
            TransactionMarker marker = new TransactionMarker(payload);

            assertTrue(marker.getDataCollections().isEmpty());
        }
    }

    private TransactionMarker parseMarker(String json) throws Exception {
        JsonNode payload = objectMapper.readTree(json);
        return new TransactionMarker(payload);
    }
}
