package org.todaybook.bookpreprocessingworker.common.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Generic JSON publisher using KafkaTemplate. Serializes payload to JSON and publishes
 * with the given topic/key. Intended to be reused across services.
 */
public class KafkaJsonPublisher<T> implements MessagePublisher<T> {

    private static final Logger log = LoggerFactory.getLogger(KafkaJsonPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaJsonPublisher(KafkaTemplate<String, Object> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(String topic, String key, T payload) {
        if (payload == null) {
            log.warn("Skip publishing null payload to topic={}", topic);
            return;
        }
        try {
            String json = objectMapper.writeValueAsString(payload);
            kafkaTemplate.send(topic, key, json)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send message. topic={}, key={}, ex={}", topic, key, ex.getMessage());
                    } else {
                        log.debug("Sent message. topic={}, key={}, offset={}", topic, key, result.getRecordMetadata().offset());
                    }
                });
        } catch (Exception e) {
            log.error("Failed to serialize payload for topic={}, key={}", topic, key, e);
        }
    }
}
