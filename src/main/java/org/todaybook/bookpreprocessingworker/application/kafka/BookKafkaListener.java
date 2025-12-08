package org.todaybook.bookpreprocessingworker.application.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookSearchResponse;
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
     * Kafka 토픽에서 네이버 도서 검색 결과 JSON을 수신하여 처리합니다.
     */
    @KafkaListener(
        topics = "${app.kafka.input-topic}",
        groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onMessage(String payload) {
        log.info(">>> [book.search.response] received payload length = {}", payload.length());

        try {
            // NaverBookSearchResponse로 역직렬화
            NaverBookSearchResponse response = objectMapper.readValue(payload, NaverBookSearchResponse.class);

            // 서비스로 전달 (null 체크는 서비스 혹은 여기서 수행)
            if (response.items() != null && !response.items().isEmpty()) {
                preprocessingService.process(response);
            } else {
                log.warn("Received empty items list from payload.");
            }

        } catch (Exception e) {
            log.error("Failed to deserialize NaverBookSearchResponse. payload fragment={}",
                payload.substring(0, Math.min(payload.length(), 100)), e);
        }
    }
}