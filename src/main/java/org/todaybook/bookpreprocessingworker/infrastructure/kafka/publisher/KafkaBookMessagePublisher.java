package org.todaybook.bookpreprocessingworker.infrastructure.kafka.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.todaybook.bookpreprocessingworker.application.dto.BookConsumeMessage;
import org.todaybook.bookpreprocessingworker.application.port.out.BookMessagePublisher;
import org.todaybook.bookpreprocessingworker.common.kafka.MessagePublisher;
import org.todaybook.bookpreprocessingworker.config.AppKafkaProperties;
import org.todaybook.bookpreprocessingworker.config.TopicNames;
import org.todaybook.bookpreprocessingworker.domain.model.Book;

/**
 * Kafka-based implementation of the outbound port for parsed books.
 * Relies on the shared JSON publisher from the common-kafka module.
 */
@Component
public class KafkaBookMessagePublisher implements BookMessagePublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaBookMessagePublisher.class);
    private static final String DEFAULT_OUTPUT_TOPIC = "book.parsed";

    private final MessagePublisher<Object> delegatePublisher;
    private final String outputTopic;

    public KafkaBookMessagePublisher(
        KafkaTemplate<String, Object> kafkaTemplate,
        ObjectMapper objectMapper,
        AppKafkaProperties appKafkaProperties,
        TopicNames topicNames
    ) {
        var jsonPublisher = new org.todaybook.bookpreprocessingworker.common.kafka.KafkaJsonPublisher<>(kafkaTemplate, objectMapper);
        this.delegatePublisher = new org.todaybook.bookpreprocessingworker.common.kafka.InstrumentedMessagePublisher<>(jsonPublisher);
        this.outputTopic = topicNames.outputTopic();
    }

    @Override
    public void publish(Book book) {
        if (book == null) {
            log.warn("Skip publishing null book");
            return;
        }

        String key = book.cleanIsbnKey();
        BookConsumeMessage message = new BookConsumeMessage(
            book.isbn(),
            book.title(),
            book.categories(),
            book.description(),
            book.author(),
            book.publisher(),
            book.publishedAt(),
            book.thumbnail()
        );

        delegatePublisher.publish(outputTopic, key, message);
    }

    private String resolveOutputTopic(AppKafkaProperties properties) {
        String configured = properties == null ? null : properties.getOutputTopic();
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        return DEFAULT_OUTPUT_TOPIC;
    }
}
