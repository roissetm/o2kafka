package com.o2kafka.aggregator.service;

import com.o2kafka.aggregator.model.ChangeEvent;
import com.o2kafka.aggregator.model.TransactionBundle;
import com.o2kafka.aggregator.model.TransactionMarker;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Core service that aggregates CDC events by transaction boundaries.
 *
 * Events are collected into TransactionBundle objects keyed by transaction ID.
 * When an END marker is received and all events have arrived, the complete
 * transaction is sent to the PostgresSinkService for atomic application.
 */
@Service
public class TransactionAggregatorService {

    private static final Logger log = LoggerFactory.getLogger(TransactionAggregatorService.class);

    private final ConcurrentHashMap<String, TransactionBundle> pendingTransactions = new ConcurrentHashMap<>();
    private final PostgresSinkService sinkService;
    private final Duration transactionTimeout;

    // Metrics
    private final Counter eventsReceivedCounter;
    private final Counter transactionsCompletedCounter;
    private final Counter transactionsFailedCounter;
    private final Counter transactionsTimedOutCounter;
    private final Timer transactionProcessingTimer;

    public TransactionAggregatorService(
            PostgresSinkService sinkService,
            MeterRegistry meterRegistry,
            @Value("${aggregator.transaction.timeout-minutes:5}") int timeoutMinutes) {

        this.sinkService = sinkService;
        this.transactionTimeout = Duration.ofMinutes(timeoutMinutes);

        // Initialize metrics
        this.eventsReceivedCounter = Counter.builder("cdc.events.received")
                .description("Total CDC events received")
                .register(meterRegistry);

        this.transactionsCompletedCounter = Counter.builder("cdc.transactions.completed")
                .description("Transactions successfully applied to PostgreSQL")
                .register(meterRegistry);

        this.transactionsFailedCounter = Counter.builder("cdc.transactions.failed")
                .description("Transactions that failed to apply")
                .register(meterRegistry);

        this.transactionsTimedOutCounter = Counter.builder("cdc.transactions.timeout")
                .description("Transactions that timed out waiting for events")
                .register(meterRegistry);

        this.transactionProcessingTimer = Timer.builder("cdc.transactions.processing.time")
                .description("Time to process complete transactions")
                .register(meterRegistry);

        // Gauge for pending transactions
        Gauge.builder("cdc.transactions.pending", pendingTransactions, Map::size)
                .description("Number of transactions waiting for completion")
                .register(meterRegistry);

        log.info("TransactionAggregatorService initialized with timeout: {} minutes", timeoutMinutes);
    }

    /**
     * Handle a CDC change event.
     * Events are grouped by their transaction ID.
     */
    public void handleChangeEvent(ChangeEvent event) {
        eventsReceivedCounter.increment();

        String txId = event.getTxId();

        // Skip events without transaction ID (shouldn't happen with provide.transaction.metadata=true)
        if (txId == null || txId.isEmpty()) {
            log.warn("Received event without transaction ID: {}", event);
            // For snapshot events, apply directly
            if (event.isSnapshot()) {
                handleSnapshotEvent(event);
            }
            return;
        }

        // Get or create transaction bundle
        TransactionBundle bundle = pendingTransactions.computeIfAbsent(
                txId,
                TransactionBundle::new
        );

        bundle.addEvent(event);

        log.debug("Added event to transaction {}: {} (now has {} events)",
                txId, event, bundle.getEventCount());

        // Check if transaction is now complete
        if (bundle.isComplete()) {
            completeTransaction(txId);
        }
    }

    /**
     * Handle a transaction marker (BEGIN or END).
     */
    public void handleTransactionMarker(TransactionMarker marker) {
        String txId = marker.getId();

        if (marker.isBegin()) {
            // Pre-create bundle for slightly better performance
            pendingTransactions.computeIfAbsent(txId, TransactionBundle::new);
            log.debug("Transaction BEGIN: {}", txId);
            return;
        }

        if (marker.isEnd()) {
            log.debug("Transaction END: {} with {} expected events", txId, marker.getEventCount());

            TransactionBundle bundle = pendingTransactions.get(txId);
            if (bundle != null) {
                bundle.markComplete(marker);

                // Check if all events already received
                if (bundle.isComplete()) {
                    completeTransaction(txId);
                } else {
                    log.debug("Transaction {} marked END but waiting for {} more events",
                            txId, marker.getEventCount() - bundle.getEventCount());
                }
            } else {
                // END arrived before any events (possible race condition)
                // Create bundle and mark complete; events will trigger completion when they arrive
                TransactionBundle newBundle = new TransactionBundle(txId);
                newBundle.markComplete(marker);
                pendingTransactions.put(txId, newBundle);
                log.debug("Created bundle for END marker {}, waiting for {} events",
                        txId, marker.getEventCount());
            }
        }
    }

    /**
     * Handle snapshot events (applied directly without transaction grouping).
     */
    private void handleSnapshotEvent(ChangeEvent event) {
        try {
            sinkService.applySingleEvent(event);
            log.debug("Applied snapshot event: {}", event);
        } catch (Exception e) {
            log.error("Failed to apply snapshot event: {}", event, e);
        }
    }

    /**
     * Complete and apply a transaction to PostgreSQL.
     */
    private void completeTransaction(String txId) {
        TransactionBundle bundle = pendingTransactions.remove(txId);

        if (bundle == null || !bundle.isComplete()) {
            return;
        }

        transactionProcessingTimer.record(() -> {
            try {
                log.info("Completing transaction {}: {} events, tables: {}",
                        txId, bundle.getEventCount(), bundle.getEventCountByTable());

                sinkService.applyTransaction(bundle);

                transactionsCompletedCounter.increment();

                log.info("Transaction {} completed successfully in {}",
                        txId, bundle.getWaitingTime());

            } catch (Exception e) {
                transactionsFailedCounter.increment();
                log.error("Transaction {} failed: {}", txId, e.getMessage(), e);
                handleFailedTransaction(bundle, e);
            }
        });
    }

    /**
     * Handle failed transactions (could send to DLT in production).
     */
    private void handleFailedTransaction(TransactionBundle bundle, Exception e) {
        log.error("Transaction {} failed with {} events. Consider sending to dead-letter topic.",
                bundle.getTxId(), bundle.getEventCount());
        // In production: send to dead-letter topic for manual review
    }

    /**
     * Handle timed-out transactions.
     */
    private void handleTimedOutTransaction(TransactionBundle bundle) {
        log.warn("Transaction {} timed out after {}. Had {} events, expected {}.",
                bundle.getTxId(),
                bundle.getWaitingTime(),
                bundle.getEventCount(),
                bundle.getExpectedCount());
        // In production: send to dead-letter topic for manual review
    }

    /**
     * Scheduled task to clean up stale transactions.
     */
    @Scheduled(fixedDelayString = "${aggregator.transaction.cleanup-interval-seconds:60}000")
    public void cleanupStaleTransactions() {
        int cleaned = 0;

        var iterator = pendingTransactions.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            TransactionBundle bundle = entry.getValue();

            if (bundle.isTimedOut(transactionTimeout)) {
                transactionsTimedOutCounter.increment();
                handleTimedOutTransaction(bundle);
                iterator.remove();
                cleaned++;
            }
        }

        if (cleaned > 0) {
            log.warn("Cleaned up {} timed-out transactions", cleaned);
        }

        if (!pendingTransactions.isEmpty()) {
            log.debug("Currently {} pending transactions", pendingTransactions.size());
        }
    }

    /**
     * Get current count of pending transactions (for monitoring).
     */
    public int getPendingTransactionCount() {
        return pendingTransactions.size();
    }
}
