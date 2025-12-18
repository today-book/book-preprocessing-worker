package org.todaybook.common.kafka;

/**
 * Minimal outbound port for publishing messages. Implementation details (Kafka, HTTP, etc.)
 * are hidden behind this interface.
 */
public interface MessagePublisher<T> {

    void publish(String topic, String key, T payload);
}
