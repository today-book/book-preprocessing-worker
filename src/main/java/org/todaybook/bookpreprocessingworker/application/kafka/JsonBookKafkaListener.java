package org.todaybook.bookpreprocessingworker.application.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;
import org.todaybook.bookpreprocessingworker.application.service.BookPreprocessingService;

@Component
public class JsonBookKafkaListener implements BookMessageListener {

    private static final Logger log = LoggerFactory.getLogger(JsonBookKafkaListener.class);

    private final BookPreprocessingService preprocessingService;
    private final ObjectMapper objectMapper;

    public JsonBookKafkaListener(BookPreprocessingService preprocessingService, ObjectMapper objectMapper) {
        this.preprocessingService = preprocessingService;
        this.objectMapper = objectMapper;
    }

    @Override
    @KafkaListener(
        topics = "${app.kafka.input-topic}",
        groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onMessage(String payload) throws JsonProcessingException {
        log.info(">>> [book.raw] received payload length = {}", payload == null ? 0 : payload.length());

        NaverBookItem item = objectMapper.readValue(payload, NaverBookItem.class);

        if (item == null) {
            log.warn("Deserialized item is null. payload={}", payload);
            return;
        }

        preprocessingService.processSingleItem(item);
    }
}
