package com.o2kafka.aggregator.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.o2kafka.aggregator.model.ChangeEvent;
import com.o2kafka.aggregator.service.TransactionAggregatorService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Kafka consumer for CDC change events from Debezium.
 * Consumes events from all table topics matching the configured pattern.
 */
@Component
public class CdcEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(CdcEventConsumer.class);

    private final TransactionAggregatorService aggregatorService;
    private final ObjectMapper objectMapper;

    public CdcEventConsumer(TransactionAggregatorService aggregatorService) {
        this.aggregatorService = aggregatorService;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Consume CDC events from all ECOM table topics.
     * Using topicPattern to match o2k.ECOM.* topics.
     */
    @KafkaListener(
            topicPattern = "${aggregator.topics.cdc-pattern:o2k\\.ECOM\\..*}",
            groupId = "${spring.kafka.consumer.group-id:tx-aggregator}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeCdcEvents(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        log.debug("Received batch of {} CDC events", records.size());

        try {
            for (ConsumerRecord<String, String> record : records) {
                processRecord(record);
            }

            // Acknowledge after successful processing
            ack.acknowledge();

        } catch (Exception e) {
            log.error("Error processing CDC event batch", e);
            // Don't acknowledge - messages will be reprocessed
            throw new RuntimeException("Failed to process CDC events", e);
        }
    }

    /**
     * Process a single CDC event record.
     */
    private void processRecord(ConsumerRecord<String, String> record) {
        String topic = record.topic();
        String value = record.value();

        // Skip tombstone messages (null value)
        if (value == null || value.isEmpty()) {
            log.trace("Skipping tombstone message from topic: {}", topic);
            return;
        }

        try {
            JsonNode root = objectMapper.readTree(value);
            JsonNode payload = root.path("payload");

            // Skip if no payload
            if (payload.isNull() || payload.isMissingNode()) {
                log.trace("Skipping message without payload from topic: {}", topic);
                return;
            }

            // Create ChangeEvent and delegate to aggregator
            ChangeEvent event = new ChangeEvent(topic, payload);

            log.trace("Processing CDC event: topic={}, op={}, txId={}",
                    topic, event.getOperation(), event.getTxId());

            aggregatorService.handleChangeEvent(event);

        } catch (Exception e) {
            log.error("Failed to process CDC event from topic {}: {}", topic, e.getMessage(), e);
            throw new RuntimeException("Failed to parse CDC event", e);
        }
    }
}
