package org.todaybook.bookpreprocessingworker.common.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;

/**
 * DefaultErrorHandler with concise logging for observability.
 */
public class LoggingErrorHandler extends DefaultErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggingErrorHandler.class);

    public LoggingErrorHandler(DeadLetterPublishingRecoverer recoverer, BackOff backOff) {
        super(recoverer, backOff);
    }

    @Override
    public void handleRemaining(Exception thrownException, java.util.List<ConsumerRecord<?, ?>> records,
                                org.apache.kafka.clients.consumer.Consumer<?, ?> consumer, org.springframework.kafka.listener.MessageListenerContainer container) {
        log.error("Kafka listener failed after retries, sending to DLT. records={}, ex={}",
            records.size(), thrownException.getMessage());
        super.handleRemaining(thrownException, records, consumer, container);
    }
}
