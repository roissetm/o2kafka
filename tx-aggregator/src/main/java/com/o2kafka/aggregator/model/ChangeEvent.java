package com.o2kafka.aggregator.model;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Objects;

/**
 * Represents a single CDC change event from Debezium.
 * Contains the before/after state, operation type, and transaction metadata.
 */
public class ChangeEvent implements Comparable<ChangeEvent> {

    private final String table;
    private final String schema;
    private final String operation;  // c=create, u=update, d=delete, r=read (snapshot)
    private final JsonNode before;
    private final JsonNode after;
    private final int totalOrder;
    private final int dataCollectionOrder;
    private final String txId;
    private final String scn;
    private final long timestampMs;

    public ChangeEvent(String topic, JsonNode eventPayload) {
        JsonNode source = eventPayload.path("source");
        JsonNode transaction = eventPayload.path("transaction");

        this.schema = source.path("schema").asText();
        this.table = source.path("table").asText();
        this.operation = eventPayload.path("op").asText();
        this.before = eventPayload.path("before");
        this.after = eventPayload.path("after");
        this.scn = source.path("scn").asText();
        this.timestampMs = eventPayload.path("ts_ms").asLong();

        // Transaction metadata (null during snapshot reads)
        if (transaction != null && !transaction.isNull() && !transaction.isMissingNode()) {
            this.txId = transaction.path("id").asText(null);
            this.totalOrder = transaction.path("total_order").asInt(0);
            this.dataCollectionOrder = transaction.path("data_collection_order").asInt(0);
        } else {
            // For snapshot events, use source.txId
            this.txId = source.path("txId").asText(null);
            this.totalOrder = 0;
            this.dataCollectionOrder = 0;
        }
    }

    @Override
    public int compareTo(ChangeEvent other) {
        return Integer.compare(this.totalOrder, other.totalOrder);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChangeEvent that = (ChangeEvent) o;
        return totalOrder == that.totalOrder &&
               Objects.equals(txId, that.txId) &&
               Objects.equals(table, that.table);
    }

    @Override
    public int hashCode() {
        return Objects.hash(txId, table, totalOrder);
    }

    // Getters
    public String getTable() { return table; }
    public String getSchema() { return schema; }
    public String getOperation() { return operation; }
    public JsonNode getBefore() { return before; }
    public JsonNode getAfter() { return after; }
    public int getTotalOrder() { return totalOrder; }
    public int getDataCollectionOrder() { return dataCollectionOrder; }
    public String getTxId() { return txId; }
    public String getScn() { return scn; }
    public long getTimestampMs() { return timestampMs; }

    public boolean isSnapshot() {
        return "r".equals(operation);
    }

    public boolean isInsert() {
        return "c".equals(operation);
    }

    public boolean isUpdate() {
        return "u".equals(operation);
    }

    public boolean isDelete() {
        return "d".equals(operation);
    }

    @Override
    public String toString() {
        return String.format("ChangeEvent{table=%s, op=%s, txId=%s, totalOrder=%d}",
                table, operation, txId, totalOrder);
    }
}
