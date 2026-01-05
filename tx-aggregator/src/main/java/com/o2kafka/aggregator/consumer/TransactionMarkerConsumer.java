package com.o2kafka.aggregator.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.o2kafka.aggregator.model.TransactionMarker;
import com.o2kafka.aggregator.service.TransactionAggregatorService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Kafka consumer for transaction markers (BEGIN/END) from Debezium.
 * Consumes from the dedicated transaction topic.
 */
@Component
public class TransactionMarkerConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransactionMarkerConsumer.class);

    private final TransactionAggregatorService aggregatorService;
    private final ObjectMapper objectMapper;

    public TransactionMarkerConsumer(TransactionAggregatorService aggregatorService) {
        this.aggregatorService = aggregatorService;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Consume transaction markers from the transaction topic.
     */
    @KafkaListener(
            topics = "${aggregator.topics.transaction:o2k.transaction}",
            groupId = "${spring.kafka.consumer.group-id:tx-aggregator}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeTransactionMarkers(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        log.debug("Received batch of {} transaction markers", records.size());

        try {
            for (ConsumerRecord<String, String> record : records) {
                processRecord(record);
            }

            // Acknowledge after successful processing
            ack.acknowledge();

        } catch (Exception e) {
            log.error("Error processing transaction marker batch", e);
            throw new RuntimeException("Failed to process transaction markers", e);
        }
    }

    /**
     * Process a single transaction marker record.
     */
    private void processRecord(ConsumerRecord<String, String> record) {
        String value = record.value();

        // Skip null messages
        if (value == null || value.isEmpty()) {
            return;
        }

        try {
            JsonNode root = objectMapper.readTree(value);
            JsonNode payload = root.path("payload");

            // Skip if no payload
            if (payload.isNull() || payload.isMissingNode()) {
                return;
            }

            // Create TransactionMarker and delegate to aggregator
            TransactionMarker marker = new TransactionMarker(payload);

            log.debug("Processing transaction marker: {} id={} events={}",
                    marker.getStatus(), marker.getId(), marker.getEventCount());

            aggregatorService.handleTransactionMarker(marker);

        } catch (Exception e) {
            log.error("Failed to process transaction marker: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to parse transaction marker", e);
        }
    }
}
