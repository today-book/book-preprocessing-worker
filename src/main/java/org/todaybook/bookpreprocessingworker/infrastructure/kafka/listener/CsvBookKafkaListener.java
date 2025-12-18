package org.todaybook.bookpreprocessingworker.infrastructure.kafka.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.todaybook.bookpreprocessingworker.application.port.in.BookMessageUseCase;
import org.todaybook.bookpreprocessingworker.config.TopicNames;

/**
 * Listener for the topic named "book.raw.csv". Payload is a raw quoted row string, not a CSV file.
 */
@Component
public class CsvBookKafkaListener implements BookMessageListener<String> {

    private static final Logger log = LoggerFactory.getLogger(CsvBookKafkaListener.class);

    private final BookMessageUseCase bookMessageUseCase;
    private final String csvInputTopic;

    public CsvBookKafkaListener(BookMessageUseCase bookMessageUseCase, TopicNames topicNames) {
        this.bookMessageUseCase = bookMessageUseCase;
        this.csvInputTopic = topicNames.csvInputTopic();
    }

    @Override
    @KafkaListener(
        topics = "#{@topicNames.csvInputTopic()}",
        groupId = "${app.kafka.csv-group-id:${spring.kafka.consumer.group-id}}",
        containerFactory = "csvKafkaListenerContainerFactory"
    )
    public void onMessage(String payload) {
        log.info(">>> [{}] received payload length = {}", csvInputTopic, payload == null ? 0 : payload.length());
        bookMessageUseCase.processRawRow(payload);
    }
}
