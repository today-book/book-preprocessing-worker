package org.todaybook.bookpreprocessingworker.infrastructure.kafka.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.todaybook.bookpreprocessingworker.application.dto.BookConsumeMessage;
import org.todaybook.bookpreprocessingworker.application.port.out.BookMessagePublisher;
import org.todaybook.bookpreprocessingworker.config.AppKafkaProperties;
import org.todaybook.bookpreprocessingworker.domain.model.Book;

@Component
public class KafkaBookMessagePublisher implements BookMessagePublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaBookMessagePublisher.class);
    private static final String DEFAULT_OUTPUT_TOPIC = "book.parsed";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String outputTopic;

    public KafkaBookMessagePublisher(
        KafkaTemplate<String, Object> kafkaTemplate,
        ObjectMapper objectMapper,
        AppKafkaProperties appKafkaProperties
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.outputTopic = resolveOutputTopic(appKafkaProperties);
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

        try {
            String jsonMessage = objectMapper.writeValueAsString(message);
            kafkaTemplate.send(outputTopic, key, jsonMessage)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send parsed book. isbn={}, ex={}", key, ex.getMessage());
                    } else {
                        log.debug("Sent parsed book. isbn={}, offset={}", key, result.getRecordMetadata().offset());
                    }
                });
        } catch (Exception e) {
            log.error("Failed to serialize message. isbn={}", key, e);
        }
    }

    private String resolveOutputTopic(AppKafkaProperties properties) {
        String configured = properties == null ? null : properties.getOutputTopic();
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        return DEFAULT_OUTPUT_TOPIC;
    }
}
