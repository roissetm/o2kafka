package com.o2kafka.aggregator.model;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Aggregates all CDC events belonging to a single database transaction.
 * Events are stored in order based on their total_order within the transaction.
 */
public class TransactionBundle {

    private final String txId;
    private final Instant startTime;
    private final Set<ChangeEvent> events;
    private volatile boolean endMarkerReceived = false;
    private volatile int expectedCount = -1;
    private volatile Map<String, Integer> expectedPerTable = Collections.emptyMap();
    private volatile long endTimestampMs;

    public TransactionBundle(String txId) {
        this.txId = txId;
        this.startTime = Instant.now();
        // ConcurrentSkipListSet maintains order based on ChangeEvent.compareTo (totalOrder)
        this.events = new ConcurrentSkipListSet<>();
    }

    /**
     * Add a CDC event to this transaction bundle.
     */
    public synchronized void addEvent(ChangeEvent event) {
        events.add(event);
    }

    /**
     * Mark this transaction as complete using the END marker metadata.
     */
    public synchronized void markComplete(TransactionMarker endMarker) {
        this.expectedCount = endMarker.getEventCount();
        this.expectedPerTable = endMarker.getDataCollections();
        this.endTimestampMs = endMarker.getTimestampMs();
        this.endMarkerReceived = true;
    }

    /**
     * Check if all expected events have been received.
     */
    public boolean isComplete() {
        return endMarkerReceived && events.size() >= expectedCount;
    }

    /**
     * Check if this transaction has been waiting too long.
     */
    public boolean isTimedOut(Duration timeout) {
        return Instant.now().isAfter(startTime.plus(timeout));
    }

    /**
     * Get the events sorted by their transaction order.
     */
    public List<ChangeEvent> getOrderedEvents() {
        return new ArrayList<>(events);
    }

    /**
     * Get count of events by table.
     */
    public Map<String, Long> getEventCountByTable() {
        Map<String, Long> counts = new HashMap<>();
        for (ChangeEvent event : events) {
            counts.merge(event.getTable(), 1L, Long::sum);
        }
        return counts;
    }

    /**
     * Validate that received events match expected counts per table.
     */
    public boolean validateEventCounts() {
        if (!endMarkerReceived) {
            return false;
        }

        Map<String, Long> actualCounts = getEventCountByTable();

        for (Map.Entry<String, Integer> expected : expectedPerTable.entrySet()) {
            Long actual = actualCounts.get(expected.getKey());
            if (actual == null || actual.intValue() != expected.getValue()) {
                return false;
            }
        }
        return true;
    }

    // Getters
    public String getTxId() { return txId; }
    public Instant getStartTime() { return startTime; }
    public int getEventCount() { return events.size(); }
    public int getExpectedCount() { return expectedCount; }
    public boolean isEndMarkerReceived() { return endMarkerReceived; }
    public long getEndTimestampMs() { return endTimestampMs; }
    public Map<String, Integer> getExpectedPerTable() { return expectedPerTable; }

    /**
     * Calculate time waiting for completion.
     */
    public Duration getWaitingTime() {
        return Duration.between(startTime, Instant.now());
    }

    @Override
    public String toString() {
        return String.format("TransactionBundle{txId=%s, events=%d/%d, complete=%s, waiting=%s}",
                txId, events.size(), expectedCount, isComplete(), getWaitingTime());
    }
}
