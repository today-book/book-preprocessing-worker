package org.todaybook.bookpreprocessingworker.infrastructure.kafka.listener;

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

    @KafkaListener(
        topics = {
            "#{@topicNames.inputTopic() + '.DLT'}",
            "#{@topicNames.csvInputTopic() + '.DLT'}"
        },
        groupId = "dlq-monitor-group",
        containerFactory = "csvKafkaListenerContainerFactory"
    )
    public void monitorDeadLetter(
        @Payload String payload,
        @Header(KafkaHeaders.DLT_ORIGINAL_TOPIC) String originalTopic,
        @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
        @Header(name = KafkaHeaders.DLT_EXCEPTION_STACKTRACE, required = false) byte[] exceptionStackTrace
    ) {
        String stackTrace = (exceptionStackTrace != null)
            ? new String(exceptionStackTrace, StandardCharsets.UTF_8)
            : "No stack trace";

        log.error("""
            🚨 [DLQ Message Arrived]
            - Original Topic: {}
            - Error Reason: {}
            - Payload: {}
            - Stack Trace: {}
            """, originalTopic, exceptionMessage, payload, stackTrace);
    }
}
