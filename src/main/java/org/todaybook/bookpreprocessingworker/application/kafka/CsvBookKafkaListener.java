package org.todaybook.bookpreprocessingworker.application.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.todaybook.bookpreprocessingworker.application.service.BookPreprocessingService;

@Component
public class CsvBookKafkaListener implements BookMessageListener {

    private static final Logger log = LoggerFactory.getLogger(CsvBookKafkaListener.class);

    private final BookPreprocessingService preprocessingService;

    public CsvBookKafkaListener(BookPreprocessingService preprocessingService) {
        this.preprocessingService = preprocessingService;
    }

    @Override
    @KafkaListener(
        topics = "${app.kafka.csv-input-topic:csv-book.raw}",
        groupId = "${spring.kafka.consumer.group-id}"
    )
    public void onMessage(String payload) {
        log.info(">>> [csv-book.raw] received payload length = {}", payload == null ? 0 : payload.length());
        preprocessingService.processCsvRow(payload);
    }
}
