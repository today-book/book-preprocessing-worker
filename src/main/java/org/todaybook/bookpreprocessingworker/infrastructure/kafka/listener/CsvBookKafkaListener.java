package org.todaybook.bookpreprocessingworker.infrastructure.kafka.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.todaybook.bookpreprocessingworker.application.port.in.BookMessageUseCase;

/**
 * Listener for the topic named "book.raw.csv". Payload is a raw quoted row string, not a CSV file.
 */
@Component
public class CsvBookKafkaListener implements BookMessageListener<String> {

    private static final Logger log = LoggerFactory.getLogger(CsvBookKafkaListener.class);

    private final BookMessageUseCase bookMessageUseCase;

    public CsvBookKafkaListener(BookMessageUseCase bookMessageUseCase) {
        this.bookMessageUseCase = bookMessageUseCase;
    }

    @Override
    @KafkaListener(
        topics = "#{@topicNames.csvInputTopic()}",
        groupId = "${app.kafka.csv-group-id:${spring.kafka.consumer.group-id}}",
        containerFactory = "csvKafkaListenerContainerFactory"
    )
    public void onMessage(String payload) {
        log.info(">>> [book.raw.csv] received payload length = {}", payload == null ? 0 : payload.length());
        bookMessageUseCase.processRawRow(payload);
    }
}
