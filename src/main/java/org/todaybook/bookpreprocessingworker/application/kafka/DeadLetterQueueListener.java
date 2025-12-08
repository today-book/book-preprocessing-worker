package org.todaybook.bookpreprocessingworker.application.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class DeadLetterQueueListener {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueListener.class);

    /**
     * DLT(Dead Letter Topic)에 들어온 메시지를 소비하여 로그를 남깁니다.
     * DB에 저장하지 않고, 카프카 토픽 자체를 저장소로 활용합니다.
     */
    @KafkaListener(
        topics = "${app.kafka.input-topic}.DLT",
        groupId = "dlq-monitor-group"
    )
    public void monitorDeadLetter(
        @Payload String payload,
        @Header(KafkaHeaders.DLT_ORIGINAL_TOPIC) String originalTopic,
        @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
        @Header(name = KafkaHeaders.DLT_EXCEPTION_STACKTRACE, required = false) byte[] exceptionStackTrace
    ) {
        // 스택트레이스 바이트 배열을 문자열로 변환 (필요 시)
        String stackTrace = (exceptionStackTrace != null)
            ? new String(exceptionStackTrace, StandardCharsets.UTF_8)
            : "No stack trace";

        // 단순히 로그만 남김 (Elasticsearch, Loki 등으로 로그 수집 시 여기서 확인 가능)
        log.error("""
            🚨 [DLQ Message Arrived]
            - Original Topic: {}
            - Error Reason: {}
            - Payload: {}
            - Stack Trace: {}
            """, originalTopic, exceptionMessage, payload, stackTrace);
    }
}