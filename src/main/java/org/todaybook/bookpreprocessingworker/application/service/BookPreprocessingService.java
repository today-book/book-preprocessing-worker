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

    /**
     * KafkaTemplate을 주입받아 서비스 인스턴스를 초기화한다.
     *
     * @param kafkaTemplate parsed 메시지를 Kafka로 전송하는 데 사용되는 KafkaTemplate
     */
    public BookPreprocessingService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * 원시 도서 메시지를 파싱하여 BookParsedMessage로 변환하고 Kafka의 "book.parsed" 토픽으로 전송한다.
     *
     * 입력이 유효하지 않거나 필수 필드(ISBN, 제목)가 비어있으면 경고 로그를 남기고 처리를 중단한다.
     * pubdate가 제공되면 yyyyMMdd 형식으로 파싱을 시도하며 파싱에 실패하면 publishedAt은 null로 유지한다.
     * Kafka 전송 결과는 비동기적으로 처리되며 전송 실패 시 오류를, 성공 시 토픽/파티션/오프셋 정보를 로그에 남긴다.
     *
     * @param raw 처리할 원시 도서 메시지 (ISBN, 제목, pubdate, 등 메타정보를 포함)
     */
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

    /**
     * 제목 문자열에서 HTML/XML 태그를 제거하고 앞뒤 공백을 제거한다.
     *
     * @param title 원본 제목 문자열 — `null`이면 그대로 `null`을 반환한다.
     * @return `title`이 `null`이면 `null`, 그렇지 않으면 태그가 제거되고 앞뒤 공백이 제거된 문자열
     */
    private String cleanTitle(String title) {
        if (title == null) return null;
        return title.replaceAll("<.*?>", "").trim();
    }
}