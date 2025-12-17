package org.todaybook.bookpreprocessingworker.infrastructure.kafka.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.todaybook.bookpreprocessingworker.application.port.in.BookMessageUseCase;

@Component
public class CsvBookKafkaListener implements BookMessageListener {

    private static final Logger log = LoggerFactory.getLogger(CsvBookKafkaListener.class);

    private final BookMessageUseCase bookMessageUseCase;

    public CsvBookKafkaListener(BookMessageUseCase bookMessageUseCase) {
        this.bookMessageUseCase = bookMessageUseCase;
    }

    @Override
    @KafkaListener(
        topics = "#{@topicNames.csvInputTopic()}",
        groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onMessage(String payload) {
        log.info(">>> [csv-book.raw] received payload length = {}", payload == null ? 0 : payload.length());
        bookMessageUseCase.processCsvRow(payload);
    }
}
