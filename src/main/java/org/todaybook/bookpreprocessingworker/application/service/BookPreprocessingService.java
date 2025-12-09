package org.todaybook.bookpreprocessingworker.application.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.common.util.StringUtils;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.todaybook.bookpreprocessingworker.application.dto.BookConsumeMessage;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookSearchResponse;

@Service
public class BookPreprocessingService {

    private static final Logger log = LoggerFactory.getLogger(BookPreprocessingService.class);
    private static final DateTimeFormatter NAVER_PUBDATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final String OUTPUT_TOPIC = "book.parsed"; // 수신 측 토픽 이름과 일치해야 함

    private final KafkaTemplate<String, Object> kafkaTemplate;

    private final ObjectMapper objectMapper; // 주입 필요

    public BookPreprocessingService(KafkaTemplate<String, Object> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public void process(NaverBookSearchResponse response) {
        List<NaverBookItem> items = response.items();

        if (items == null || items.isEmpty()) {
            log.warn("Received empty item list. Total hits: {}", response.total());
            return;
        }

        log.info(">>> Processing batch. Total hits: {}, Current batch size: {}",
            response.total(), items.size());

        for (NaverBookItem item : items) {
            processSingleItem(item);
        }
    }

    public void processSingleItem(NaverBookItem item) {
        String rawIsbn = item.isbn();
        String title = item.title();

        if (StringUtils.isBlank(rawIsbn) || StringUtils.isBlank(title)) {
            log.debug("Skipping item with empty ISBN or Title. item={}", item);
            return;
        }

        // 1. ISBN 파싱 (공백 분리 및 13자리 우선 추출)
        String refinedIsbn = extractIsbn13(rawIsbn);

        // 2. 날짜 변환 (String -> LocalDate -> LocalDateTime)
        LocalDate publishedAt = parsePublishDateToDate(item.pubdate(), refinedIsbn);

        // 3. 메시지 생성
        // categories는 네이버 API 기본 응답에 없으므로 빈 리스트 처리
        BookConsumeMessage message = new BookConsumeMessage(
            refinedIsbn,
            cleanTitle(title),
            Collections.emptyList(),
            item.description(),
            item.author(),      // 필요하다면 .replace("^", ", ") 처리 가능
            item.publisher(),
            publishedAt,
            item.image()        // thumbnail 매핑
        );

        // 4. Kafka 전송
        sendToKafka(refinedIsbn, message);
    }

    private void sendToKafka(String key, BookConsumeMessage message) {
        try {
            // [수정] 객체(message)를 JSON 문자열로 직접 변환하여 전송
            String jsonMessage = objectMapper.writeValueAsString(message);

            kafkaTemplate.send(OUTPUT_TOPIC, key, jsonMessage)
                .whenComplete((result, ex) -> {
                    // ... 로깅 로직 ...
                });
        } catch (Exception e) {
            log.error("Failed to serialize message. isbn={}", key, e);
        }
    }

    // --- Helper Methods ---

    /**
     * yyyyMMdd 문자열을 LocalDate으로 변환
     */
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
        if (!rawIsbn.contains(" ")) {
            return rawIsbn.trim();
        }
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