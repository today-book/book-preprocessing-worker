package org.todaybook.bookpreprocessingworker.application.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;
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
    public void onMessage(String payload) throws JsonProcessingException {
        // try-catch 제거! 예외가 터지면 Spring Kafka ErrorHandler에게 맡김.

        log.info(">>> [book.search.response] received payload length = {}", payload.length());

        // 1. 역직렬화 수행 (여기서 실패하면 JsonProcessingException 발생 -> 재시도 -> DLQ)
        // NaverBookSearchResponse response = objectMapper.readValue(payload, NaverBookSearchResponse.class);

        // 1. 역직렬화 다건
        NaverBookItem item = objectMapper.readValue(payload, NaverBookItem.class);

        // 2. 비즈니스 로직 다건
        preprocessingService.processSingleItem(item);

        // 2. 비즈니스 로직 수행 (여기서 RuntimeException 발생 -> 재시도 -> DLQ)
        //        if (response.items() != null && !response.items().isEmpty()) {
        //            preprocessingService.process(response);
        //        } else {
        //            log.warn("Received empty items list.");
        //        }
    }
}