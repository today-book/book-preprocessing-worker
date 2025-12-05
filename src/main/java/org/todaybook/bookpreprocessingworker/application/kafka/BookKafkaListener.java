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

    /**
     * BookKafkaListener의 의존성을 주입하고 내부 필드를 초기화합니다.
     *
     * @param preprocessingService 원시 도서 메시지 전처리를 수행하는 서비스
     * @param objectMapper JSON 직렬화/역직렬화를 수행하는 Jackson ObjectMapper
     */
    public BookKafkaListener(BookPreprocessingService preprocessingService,
        ObjectMapper objectMapper) {
        this.preprocessingService = preprocessingService;
        this.objectMapper = objectMapper;
    }

    /**
     * Kafka 토픽에서 수신한 원시 도서 메시지 페이로드를 역직렬화하여 전처리 서비스로 전달한다.
     *
     * 페이로드가 BookRawMessage로 성공적으로 역직렬화되면 전처리 로직을 실행한다.
     * 역직렬화 또는 처리 중 오류가 발생하면 페이로드와 예외 정보를 함께 기록한다.
     *
     * @param payload 수신된 메시지의 JSON 문자열(예상 형태: BookRawMessage)
     */
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