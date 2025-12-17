package org.todaybook.bookpreprocessingworker.common.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decorator that adds simple logging around publish calls for observability.
 */
public class InstrumentedMessagePublisher<T> implements MessagePublisher<T> {

    private static final Logger log = LoggerFactory.getLogger(InstrumentedMessagePublisher.class);

    private final MessagePublisher<T> delegate;

    public InstrumentedMessagePublisher(MessagePublisher<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void publish(String topic, String key, T payload) {
        long start = System.currentTimeMillis();
        try {
            delegate.publish(topic, key, payload);
        } finally {
            long elapsed = System.currentTimeMillis() - start;
            log.debug("Published topic={} key={} elapsedMs={}", topic, key, elapsed);
        }
    }
}
