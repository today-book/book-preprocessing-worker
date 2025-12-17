package org.todaybook.bookpreprocessingworker.application.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;
import org.todaybook.bookpreprocessingworker.application.service.BookPreprocessingService;
import io.micrometer.common.util.StringUtils;

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
        topics = {
            "${app.kafka.input-topic}",
            "${app.kafka.csv-input-topic:csv-book.raw}"
        },
        groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onMessage(String payload) throws JsonProcessingException {
        // payload 로그는 디버그 레벨이나 필요시에만 남기는 것이 좋습니다.
        log.info(">>> [book.item] received payload length = {}", payload == null ? 0 : payload.length());

        // CSV 기반 메시지라면 바로 CSV 파서로 전달
        if (isCsvPayload(payload)) {
            preprocessingService.processCsvRow(payload);
            return;
        }

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

    private boolean isCsvPayload(String payload) {
        if (payload == null) {
            throw new IllegalArgumentException("Payload must not be null");
        }
        String trimmed = payload.trim();
        if (StringUtils.isBlank(trimmed)) {
            return false;
        }
        if (trimmed.startsWith("{") || trimmed.startsWith("[")) {
            return false;
        }
        // 기본적으로 쉼표와 따옴표가 있는 문자열을 CSV로 간주
        return trimmed.contains(",") && trimmed.contains("\"");
    }
}
