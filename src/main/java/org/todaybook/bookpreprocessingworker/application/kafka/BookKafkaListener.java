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
public class BookKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(BookKafkaListener.class);

    private final BookPreprocessingService preprocessingService;
    private final ObjectMapper objectMapper;

    public BookKafkaListener(BookPreprocessingService preprocessingService, ObjectMapper objectMapper) {
        this.preprocessingService = preprocessingService;
        this.objectMapper = objectMapper;
    }

    /**
     * Kafka 토픽에서 단건 도서 정보(JSON)를 수신하여 처리합니다.
     */
    @KafkaListener(
        topics = "${app.kafka.input-topic}",
        groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onMessage(String payload) throws JsonProcessingException {
        // payload 로그는 디버그 레벨이나 필요시에만 남기는 것이 좋습니다.
        log.info(">>> [book.item] received payload length = {}", payload.length());

        // 1. 단건 역직렬화 (실패 시 예외 발생 -> DLQ 이동)
        NaverBookItem item = objectMapper.readValue(payload, NaverBookItem.class);

        // 2. 방어 로직: 역직렬화 결과가 null인 경우 (거의 없지만 안전장치)
        if (item == null) {
            log.warn("Deserialized item is null. payload={}", payload);
            return;
        }

        // 3. 비즈니스 로직 수행 (단건 처리 메서드 호출)
        preprocessingService.processSingleItem(item);
    }
}