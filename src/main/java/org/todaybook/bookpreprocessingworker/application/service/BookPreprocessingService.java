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
import org.todaybook.bookpreprocessingworker.config.AppKafkaProperties;

@Service
public class BookPreprocessingService {

    private static final Logger log = LoggerFactory.getLogger(BookPreprocessingService.class);
    private static final DateTimeFormatter NAVER_PUBDATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter CSV_PUBDATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final int CSV_ISBN_13_INDEX = 1;
    private static final int CSV_TITLE_INDEX = 3;
    private static final int CSV_AUTHOR_INDEX = 4;
    private static final int CSV_PUBLISHER_INDEX = 5;
    private static final int CSV_IMAGE_INDEX = 9;
    private static final int CSV_DESCRIPTION_INDEX = 11;
    private static final int CSV_PUBDATE_INDEX = 14;
    private static final int CSV_FALLBACK_ISBN_INDEX = 17;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String outputTopic;

    public BookPreprocessingService(
        KafkaTemplate<String, Object> kafkaTemplate,
        ObjectMapper objectMapper,
        AppKafkaProperties appKafkaProperties
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.outputTopic = resolveOutputTopic(appKafkaProperties);
    }

    /**
     * Preprocesses a single CSV row representing raw book data and publishes it as a BookConsumeMessage.
     *
     * @param csvRow a single line CSV string (quoted) containing book information
     */
    public void processCsvRow(String csvRow) {
        if (StringUtils.isBlank(csvRow)) {
            log.warn("Skipping empty CSV row.");
            return;
        }

        var columns = parseCsvColumns(csvRow);
        if (columns.isEmpty()) {
            log.warn("Failed to parse CSV row: {}", csvRow);
            return;
        }

        String isbn = extractCsvIsbn(columns);
        String title = getColumn(columns, CSV_TITLE_INDEX);
        String author = getColumn(columns, CSV_AUTHOR_INDEX);
        String publisher = getColumn(columns, CSV_PUBLISHER_INDEX);
        String description = firstNonBlank(
            getColumn(columns, CSV_DESCRIPTION_INDEX),
            getColumn(columns, CSV_DESCRIPTION_INDEX + 1) // slight variation slot if description shifts
        );
        String thumbnail = getColumn(columns, CSV_IMAGE_INDEX);
        LocalDate publishedAt = parseCsvPublishDate(getColumn(columns, CSV_PUBDATE_INDEX), isbn);

        if (StringUtils.isBlank(isbn) || StringUtils.isBlank(title)) {
            log.info("Skipping CSV row due to missing required fields. isbn={}, title={}", isbn, title);
            return;
        }

        BookConsumeMessage message = new BookConsumeMessage(
            isbn,
            cleanTitle(title),
            Collections.emptyList(),
            description,
            author,
            publisher,
            publishedAt,
            thumbnail
        );

        sendToKafka(isbn, message);
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

    private LocalDate parseCsvPublishDate(String pubdateStr, String isbn) {
        if (StringUtils.isBlank(pubdateStr)) return null;
        try {
            return LocalDate.parse(pubdateStr, CSV_PUBDATE_FORMAT);
        } catch (Exception e) {
            log.warn("Failed to parse CSV pubdate='{}' for isbn={}", pubdateStr, isbn);
            return null;
        }
    }

    private String extractCsvIsbn(java.util.List<String> columns) {
        String primary = getColumn(columns, CSV_ISBN_13_INDEX);
        String fallback = getColumn(columns, CSV_FALLBACK_ISBN_INDEX);

        String normalizedPrimary = normalizeIsbn(primary);
        if (normalizedPrimary != null) return normalizedPrimary;

        return normalizeIsbn(fallback);
    }

    private String normalizeIsbn(String raw) {
        if (StringUtils.isBlank(raw)) return null;
        String digitsOnly = raw.replaceAll("[^0-9Xx]", "");
        if (digitsOnly.length() >= 13) return digitsOnly.substring(0, 13);
        if (digitsOnly.length() == 10) return digitsOnly;
        return raw.trim();
    }

    private java.util.List<String> parseCsvColumns(String row) {
        java.util.List<String> columns = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < row.length(); i++) {
            char c = row.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < row.length() && row.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                columns.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        columns.add(current.toString().trim());

        return columns;
    }

    private String getColumn(java.util.List<String> columns, int index) {
        if (index < 0 || index >= columns.size()) return null;
        String value = columns.get(index);
        return StringUtils.isBlank(value) ? null : value.trim();
    }

    private String firstNonBlank(String... candidates) {
        for (String candidate : candidates) {
            if (StringUtils.isNotBlank(candidate)) return candidate.trim();
        }
        return null;
    }

    private boolean isInvalidItem(String isbn, String title, String author, String description) {
        return StringUtils.isBlank(isbn)
            || StringUtils.isBlank(title)
            || StringUtils.isBlank(author)
            || StringUtils.isBlank(description);
    }

    private void sendToKafka(String key, BookConsumeMessage message) {
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

    private String resolveOutputTopic(AppKafkaProperties properties) {
        String configured = properties == null ? null : properties.getOutputTopic();
        if (StringUtils.isNotBlank(configured)) {
            return configured;
        }
        return "book.parsed";
    }
}
