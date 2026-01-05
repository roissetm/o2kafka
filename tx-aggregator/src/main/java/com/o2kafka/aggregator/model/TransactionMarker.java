package com.o2kafka.aggregator.model;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a transaction BEGIN or END marker from the transaction topic.
 * END markers contain the total event count and per-table breakdown.
 */
public class TransactionMarker {

    public enum Status {
        BEGIN, END
    }

    private final Status status;
    private final String id;
    private final int eventCount;
    private final Map<String, Integer> dataCollections;
    private final long timestampMs;

    public TransactionMarker(JsonNode payload) {
        String statusStr = payload.path("status").asText();
        this.status = "BEGIN".equalsIgnoreCase(statusStr) ? Status.BEGIN : Status.END;
        this.id = payload.path("id").asText();
        this.eventCount = payload.path("event_count").asInt(0);
        this.dataCollections = parseDataCollections(payload.path("data_collections"));
        this.timestampMs = payload.path("ts_ms").asLong();
    }

    private Map<String, Integer> parseDataCollections(JsonNode node) {
        Map<String, Integer> result = new HashMap<>();
        if (node != null && node.isArray()) {
            for (JsonNode item : node) {
                String collection = item.path("data_collection").asText();
                int count = item.path("event_count").asInt();
                // Extract table name from full path (e.g., "XEPDB1.ECOM.ORDERS" -> "ORDERS")
                String tableName = collection.contains(".") ?
                        collection.substring(collection.lastIndexOf('.') + 1) : collection;
                result.put(tableName, count);
            }
        }
        return result;
    }

    public boolean isBegin() {
        return status == Status.BEGIN;
    }

    public boolean isEnd() {
        return status == Status.END;
    }

    // Getters
    public Status getStatus() { return status; }
    public String getId() { return id; }
    public int getEventCount() { return eventCount; }
    public Map<String, Integer> getDataCollections() { return dataCollections; }
    public long getTimestampMs() { return timestampMs; }

    @Override
    public String toString() {
        return String.format("TransactionMarker{status=%s, id=%s, eventCount=%d}",
                status, id, eventCount);
    }
}
