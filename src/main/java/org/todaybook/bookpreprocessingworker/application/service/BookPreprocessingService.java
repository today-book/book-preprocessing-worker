package org.todaybook.bookpreprocessingworker.application.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.common.util.StringUtils;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.todaybook.bookpreprocessingworker.application.dto.BookConsumeMessage;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;

@Service
public class BookPreprocessingService {

    private static final Logger log = LoggerFactory.getLogger(BookPreprocessingService.class);
    private static final DateTimeFormatter NAVER_PUBDATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final String OUTPUT_TOPIC = "book.parsed";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public BookPreprocessingService(KafkaTemplate<String, Object> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * [변경] 반복문 메서드(process) 삭제됨.
     * 단건 처리 메서드만 남겨둡니다.
     */
    public void processSingleItem(NaverBookItem item) {
        String rawIsbn = item.isbn();
        String title = item.title();
        String author = item.author();
        String description = item.description();

        // 필수 필드 검증 (비어있으면 로직 종료)
        if (isInvalidItem(rawIsbn, title, author, description)) {
            log.info("Skipping invalid item (missing required fields). ISBN={}, Title={}", rawIsbn, title);
            return;
        }

        // 1. ISBN 파싱
        String refinedIsbn = extractIsbn13(rawIsbn);

        // 2. 날짜 변환
        LocalDate publishedAt = parsePublishDateToDate(item.pubdate(), refinedIsbn);

        // 3. 메시지 생성
        BookConsumeMessage message = new BookConsumeMessage(
            refinedIsbn,
            cleanTitle(title),
            Collections.emptyList(),
            description,
            author,
            item.publisher(),
            publishedAt,
            item.image()
        );

        // 4. Kafka 전송
        sendToKafka(refinedIsbn, message);
    }

    // --- Helper Methods (기존과 동일) ---

    private boolean isInvalidItem(String isbn, String title, String author, String description) {
        return StringUtils.isBlank(isbn)
            || StringUtils.isBlank(title)
            || StringUtils.isBlank(author)
            || StringUtils.isBlank(description);
    }

    private void sendToKafka(String key, BookConsumeMessage message) {
        try {
            String jsonMessage = objectMapper.writeValueAsString(message);
            kafkaTemplate.send(OUTPUT_TOPIC, key, jsonMessage)
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

    private LocalDate parsePublishDateToDate(String pubdateStr, String isbn) {
        if (StringUtils.isBlank(pubdateStr)) return null;
        try {
            return LocalDate.parse(pubdateStr, NAVER_PUBDATE_FORMAT);
        } catch (Exception e) {
            log.warn("Failed to parse pubdate='{}' for isbn={}", pubdateStr, isbn);
            return null;
        }
    }

    private String extractIsbn13(String rawIsbn) {
        if (!rawIsbn.contains(" ")) return rawIsbn.trim();
        String[] tokens = rawIsbn.split(" ");
        for (String token : tokens) {
            if (token.length() == 13) return token;
        }
        return tokens[0];
    }

    private String cleanTitle(String title) {
        if (title == null) return null;
        return title.replaceAll("<.*?>", "").trim();
    }
}