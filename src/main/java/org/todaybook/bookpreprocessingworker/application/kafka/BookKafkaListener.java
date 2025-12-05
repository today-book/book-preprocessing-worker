package org.todaybook.bookpreprocessingworker.application.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.todaybook.bookpreprocessingworker.application.dto.BookRawMessage;
import org.todaybook.bookpreprocessingworker.application.service.BookPreprocessingService;

@Component
public class BookKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(BookKafkaListener.class);

    private final BookPreprocessingService preprocessingService;
    private final ObjectMapper objectMapper;

    public BookKafkaListener(BookPreprocessingService preprocessingService,
        ObjectMapper objectMapper) {
        this.preprocessingService = preprocessingService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
        topics = "${app.kafka.input-topic}",
        groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onMessage(String payload) {
        log.info(">>> [book.raw] received payload = {}", payload);

        try {
            BookRawMessage raw = objectMapper.readValue(payload, BookRawMessage.class);
            preprocessingService.process(raw);
        } catch (Exception e) {
            log.error("Failed to deserialize BookRawMessage. payload={}", payload, e);
        }
    }
}