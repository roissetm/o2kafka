package com.o2kafka.aggregator.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.o2kafka.aggregator.model.ChangeEvent;
import com.o2kafka.aggregator.model.TransactionMarker;
import com.o2kafka.aggregator.model.TransactionBundle;
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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for TransactionAggregatorService.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("TransactionAggregatorService")
class TransactionAggregatorServiceTest {

    @Mock
    private PostgresSinkService sinkService;

    private TransactionAggregatorService aggregatorService;
    private ObjectMapper objectMapper;

    @Captor
    private ArgumentCaptor<TransactionBundle> bundleCaptor;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        aggregatorService = new TransactionAggregatorService(
                sinkService,
                new SimpleMeterRegistry(),
                5 // 5 minute timeout
        );
    }

    @Nested
    @DisplayName("Transaction lifecycle")
    class TransactionLifecycleTests {

        @Test
        @DisplayName("Should create new transaction on BEGIN marker")
        void shouldCreateNewTransactionOnBeginMarker() throws Exception {
            TransactionMarker beginMarker = createBeginMarker("tx001");

            aggregatorService.handleTransactionMarker(beginMarker);

            // No sink call yet - transaction just started
            verify(sinkService, never()).applyTransaction(any());
        }

        @Test
        @DisplayName("Should complete transaction when all events received after END")
        void shouldCompleteTransactionWhenAllEventsReceivedAfterEnd() throws Exception {
            String txId = "tx002";

            // BEGIN
            aggregatorService.handleTransactionMarker(createBeginMarker(txId));

            // Events
            aggregatorService.handleChangeEvent(createEvent(txId, "ORDERS", 1));
            aggregatorService.handleChangeEvent(createEvent(txId, "ORDER_ITEMS", 2));
            aggregatorService.handleChangeEvent(createEvent(txId, "PAYMENTS", 3));

            // END with 3 events
            aggregatorService.handleTransactionMarker(createEndMarker(txId, 3));

            // Should have applied the transaction
            verify(sinkService, times(1)).applyTransaction(bundleCaptor.capture());

            var bundle = bundleCaptor.getValue();
            assertEquals(txId, bundle.getTxId());
            assertEquals(3, bundle.getEventCount());
        }

        @Test
        @DisplayName("Should complete transaction when END arrives before all events")
        void shouldCompleteTransactionWhenEndArrivesBeforeAllEvents() throws Exception {
            String txId = "tx003";

            // BEGIN
            aggregatorService.handleTransactionMarker(createBeginMarker(txId));

            // Only 1 event first
            aggregatorService.handleChangeEvent(createEvent(txId, "ORDERS", 1));

            // END arrives (expecting 3 events)
            aggregatorService.handleTransactionMarker(createEndMarker(txId, 3));

            // Not complete yet
            verify(sinkService, never()).applyTransaction(any());

            // Remaining events arrive
            aggregatorService.handleChangeEvent(createEvent(txId, "ORDER_ITEMS", 2));
            aggregatorService.handleChangeEvent(createEvent(txId, "PAYMENTS", 3));

            // Now should be complete
            verify(sinkService, times(1)).applyTransaction(any());
        }

        @Test
        @DisplayName("Should handle events arriving before BEGIN marker")
        void shouldHandleEventsArrivingBeforeBeginMarker() throws Exception {
            String txId = "tx004";

            // Events arrive first (creates implicit transaction)
            aggregatorService.handleChangeEvent(createEvent(txId, "ORDERS", 1));
            aggregatorService.handleChangeEvent(createEvent(txId, "ORDER_ITEMS", 2));

            // BEGIN arrives (should find existing transaction)
            aggregatorService.handleTransactionMarker(createBeginMarker(txId));

            // More events
            aggregatorService.handleChangeEvent(createEvent(txId, "PAYMENTS", 3));

            // END
            aggregatorService.handleTransactionMarker(createEndMarker(txId, 3));

            verify(sinkService, times(1)).applyTransaction(bundleCaptor.capture());
            assertEquals(3, bundleCaptor.getValue().getEventCount());
        }
    }

    @Nested
    @DisplayName("Snapshot event handling")
    class SnapshotEventTests {

        @Test
        @DisplayName("Should apply snapshot events immediately")
        void shouldApplySnapshotEventsImmediately() throws Exception {
            ChangeEvent snapshotEvent = createSnapshotEvent("PRODUCTS");

            aggregatorService.handleChangeEvent(snapshotEvent);

            verify(sinkService, times(1)).applySingleEvent(snapshotEvent);
        }

        @Test
        @DisplayName("Should not buffer snapshot events")
        void shouldNotBufferSnapshotEvents() throws Exception {
            // Multiple snapshot events
            aggregatorService.handleChangeEvent(createSnapshotEvent("PRODUCTS"));
            aggregatorService.handleChangeEvent(createSnapshotEvent("CATEGORIES"));
            aggregatorService.handleChangeEvent(createSnapshotEvent("CUSTOMERS"));

            // Each should be applied individually
            verify(sinkService, times(3)).applySingleEvent(any());
            verify(sinkService, never()).applyTransaction(any());
        }
    }

    @Nested
    @DisplayName("Event ordering")
    class EventOrderingTests {

        @Test
        @DisplayName("Should apply events in total_order sequence")
        void shouldApplyEventsInTotalOrderSequence() throws Exception {
            String txId = "tx005";

            aggregatorService.handleTransactionMarker(createBeginMarker(txId));

            // Events arrive out of order
            aggregatorService.handleChangeEvent(createEvent(txId, "PAYMENTS", 5));
            aggregatorService.handleChangeEvent(createEvent(txId, "ORDERS", 1));
            aggregatorService.handleChangeEvent(createEvent(txId, "INVENTORY", 3));
            aggregatorService.handleChangeEvent(createEvent(txId, "ORDER_ITEMS", 2));
            aggregatorService.handleChangeEvent(createEvent(txId, "INVENTORY", 4));

            aggregatorService.handleTransactionMarker(createEndMarker(txId, 5));

            verify(sinkService).applyTransaction(bundleCaptor.capture());

            var orderedEvents = bundleCaptor.getValue().getOrderedEvents();
            assertEquals(1L, orderedEvents.get(0).getTotalOrder());
            assertEquals(2L, orderedEvents.get(1).getTotalOrder());
            assertEquals(3L, orderedEvents.get(2).getTotalOrder());
            assertEquals(4L, orderedEvents.get(3).getTotalOrder());
            assertEquals(5L, orderedEvents.get(4).getTotalOrder());
        }
    }

    @Nested
    @DisplayName("Multiple concurrent transactions")
    class ConcurrentTransactionTests {

        @Test
        @DisplayName("Should handle interleaved events from multiple transactions")
        void shouldHandleInterleavedEventsFromMultipleTransactions() throws Exception {
            String tx1 = "tx-A";
            String tx2 = "tx-B";

            // Start both transactions
            aggregatorService.handleTransactionMarker(createBeginMarker(tx1));
            aggregatorService.handleTransactionMarker(createBeginMarker(tx2));

            // Interleaved events
            aggregatorService.handleChangeEvent(createEvent(tx1, "ORDERS", 1));
            aggregatorService.handleChangeEvent(createEvent(tx2, "CUSTOMERS", 1));
            aggregatorService.handleChangeEvent(createEvent(tx1, "ORDER_ITEMS", 2));
            aggregatorService.handleChangeEvent(createEvent(tx2, "CUSTOMERS", 2));

            // End tx2 first
            aggregatorService.handleTransactionMarker(createEndMarker(tx2, 2));

            verify(sinkService, times(1)).applyTransaction(bundleCaptor.capture());
            assertEquals(tx2, bundleCaptor.getValue().getTxId());
            assertEquals(2, bundleCaptor.getValue().getEventCount());

            // End tx1
            aggregatorService.handleTransactionMarker(createEndMarker(tx1, 2));

            verify(sinkService, times(2)).applyTransaction(any());
        }

        @Test
        @DisplayName("Should isolate events between transactions")
        void shouldIsolateEventsBetweenTransactions() throws Exception {
            String tx1 = "tx-X";
            String tx2 = "tx-Y";

            aggregatorService.handleTransactionMarker(createBeginMarker(tx1));
            aggregatorService.handleChangeEvent(createEvent(tx1, "ORDERS", 1));
            aggregatorService.handleChangeEvent(createEvent(tx1, "ORDERS", 2));
            aggregatorService.handleTransactionMarker(createEndMarker(tx1, 2));

            aggregatorService.handleTransactionMarker(createBeginMarker(tx2));
            aggregatorService.handleChangeEvent(createEvent(tx2, "PRODUCTS", 1));
            aggregatorService.handleTransactionMarker(createEndMarker(tx2, 1));

            verify(sinkService, times(2)).applyTransaction(bundleCaptor.capture());

            var bundles = bundleCaptor.getAllValues();
            assertEquals(2, bundles.get(0).getEventCount()); // tx1
            assertEquals(1, bundles.get(1).getEventCount()); // tx2
        }
    }

    @Nested
    @DisplayName("Error handling")
    class ErrorHandlingTests {

        @Test
        @DisplayName("Should handle events without transaction ID")
        void shouldHandleEventsWithoutTransactionId() throws Exception {
            ChangeEvent eventWithoutTxId = createSnapshotEvent("PRODUCTS");

            // Should not throw
            assertDoesNotThrow(() -> aggregatorService.handleChangeEvent(eventWithoutTxId));
        }

        @Test
        @DisplayName("Should handle duplicate BEGIN markers")
        void shouldHandleDuplicateBeginMarkers() throws Exception {
            String txId = "tx-dup";

            aggregatorService.handleTransactionMarker(createBeginMarker(txId));
            aggregatorService.handleChangeEvent(createEvent(txId, "ORDERS", 1));

            // Duplicate BEGIN (should be ignored)
            aggregatorService.handleTransactionMarker(createBeginMarker(txId));

            aggregatorService.handleTransactionMarker(createEndMarker(txId, 1));

            verify(sinkService, times(1)).applyTransaction(bundleCaptor.capture());
            assertEquals(1, bundleCaptor.getValue().getEventCount());
        }
    }

    // Helper methods

    private TransactionMarker createBeginMarker(String txId) throws Exception {
        String json = String.format("""
            {"status": "BEGIN", "id": "%s", "event_count": null}
            """, txId);
        return new TransactionMarker(objectMapper.readTree(json));
    }

    private TransactionMarker createEndMarker(String txId, int eventCount) throws Exception {
        String json = String.format("""
            {"status": "END", "id": "%s", "event_count": %d}
            """, txId, eventCount);
        return new TransactionMarker(objectMapper.readTree(json));
    }

    private ChangeEvent createEvent(String txId, String table, long totalOrder) throws Exception {
        String json = String.format("""
            {
                "op": "c",
                "after": {"ID": 1},
                "source": {"table": "%s"},
                "transaction": {
                    "id": "%s",
                    "total_order": %d,
                    "data_collection_order": 1
                }
            }
            """, table, txId, totalOrder);
        return new ChangeEvent("o2k.ECOM." + table, objectMapper.readTree(json));
    }

    private ChangeEvent createSnapshotEvent(String table) throws Exception {
        String json = String.format("""
            {
                "op": "r",
                "after": {"ID": 1},
                "source": {"table": "%s", "snapshot": "true"}
            }
            """, table);
        return new ChangeEvent("o2k.ECOM." + table, objectMapper.readTree(json));
    }
}
