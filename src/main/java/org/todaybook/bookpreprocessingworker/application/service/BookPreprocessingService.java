package org.todaybook.bookpreprocessingworker.application.service;

import io.micrometer.common.util.StringUtils;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.todaybook.bookpreprocessingworker.application.dto.BookParsedMessage;
import org.todaybook.bookpreprocessingworker.application.dto.BookRawMessage;

@Service
public class BookPreprocessingService {

    private static final Logger log = LoggerFactory.getLogger(BookPreprocessingService.class);
    private static final DateTimeFormatter NAVER_PUBDATE_FORMAT =
        DateTimeFormatter.ofPattern("yyyyMMdd");

    // ✅ 제네릭 타입 수정
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public BookPreprocessingService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void process(BookRawMessage raw) {
        if (raw == null || StringUtils.isBlank(raw.isbn()) || StringUtils.isBlank(raw.title())) {
            log.warn("Skipping book with null/blank title or isbn. raw={}", raw);
            return;
        }

        LocalDate publishedAt = null;
        if (StringUtils.isNotBlank(raw.pubdate())) {
            try {
                publishedAt = LocalDate.parse(raw.pubdate(), NAVER_PUBDATE_FORMAT);
            } catch (Exception e) {
                log.warn("Failed to parse pubdate='{}' as yyyyMMdd. isbn={}",
                    raw.pubdate(), raw.isbn(), e);
            }
        }

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        BookParsedMessage parsed = new BookParsedMessage(
            raw.isbn(),
            cleanTitle(raw.title()),
            raw.thumbnail(),
            raw.description(),
            raw.publisher(),
            publishedAt,
            now,
            now
        );

        String outputTopic = "book.parsed"; // 나중에 app.kafka.output-topic 으로 빼도 됨

        kafkaTemplate.send(outputTopic, raw.isbn(), parsed)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send parsed book. isbn={}, parsed={}",
                        raw.isbn(), parsed, ex);
                } else {
                    RecordMetadata meta = result.getRecordMetadata();
                    log.info(
                        "Sent parsed book to topic={}, partition={}, offset={}, key={}, isbn={}",
                        meta.topic(), meta.partition(), meta.offset(),
                        raw.isbn(), raw.isbn()
                    );
                }
            });
    }

    private String cleanTitle(String title) {
        if (title == null) return null;
        return title.replaceAll("<.*?>", "").trim();
    }
}