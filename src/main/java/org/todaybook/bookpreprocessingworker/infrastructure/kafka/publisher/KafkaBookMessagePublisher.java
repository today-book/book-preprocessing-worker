package org.todaybook.bookpreprocessingworker.infrastructure.kafka.publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.todaybook.bookpreprocessingworker.application.port.out.BookMessagePublisher;
import org.todaybook.bookpreprocessingworker.config.TopicNames;
import org.todaybook.bookpreprocessingworker.domain.model.Book;

/**
 * Kafka-based implementation of the outbound port for parsed books.
 */
@Component
public class KafkaBookMessagePublisher implements BookMessagePublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaBookMessagePublisher.class);

    private final KafkaTemplate<String, Book> kafkaTemplate;
    private final String outputTopic;

    public KafkaBookMessagePublisher(KafkaTemplate<String, Book> kafkaTemplate, TopicNames topicNames) {
        this.kafkaTemplate = kafkaTemplate;
        this.outputTopic = topicNames.outputTopic();
    }

    @Override
    public void publish(Book book) {
        if (book == null) {
            log.warn("Skip publishing null book");
            return;
        }

        String key = book.cleanIsbnKey();
        kafkaTemplate.send(outputTopic, book);
        log.info("Published book. isbn={}, topic={}", book.isbn(), outputTopic);
    }
}
