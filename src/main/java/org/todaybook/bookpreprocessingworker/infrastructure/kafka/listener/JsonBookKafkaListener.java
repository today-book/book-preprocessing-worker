package org.todaybook.bookpreprocessingworker.infrastructure.kafka.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;
import org.todaybook.bookpreprocessingworker.application.port.in.BookMessageUseCase;
import org.todaybook.bookpreprocessingworker.config.TopicNames;

@Component
public class JsonBookKafkaListener implements BookMessageListener<NaverBookItem> {

    private static final Logger log = LoggerFactory.getLogger(JsonBookKafkaListener.class);

    private final BookMessageUseCase bookMessageUseCase;
    private final String inputTopic;

    public JsonBookKafkaListener(BookMessageUseCase bookMessageUseCase, TopicNames topicNames) {
        this.bookMessageUseCase = bookMessageUseCase;
        this.inputTopic = topicNames.inputTopic();
    }

    @Override
    @KafkaListener(
        topics = "#{@topicNames.inputTopic()}",
        groupId = "${app.kafka.json-group-id:${spring.kafka.consumer.group-id}}",
        containerFactory = "jsonKafkaListenerContainerFactory"
    )
    public void onMessage(NaverBookItem payload) {
        if (payload == null) {
            log.warn(">>> [{}] received null payload", inputTopic);
            return;
        }

        log.info(">>> [{}] received isbn={}", inputTopic, payload.isbn());
        bookMessageUseCase.processSingleItem(payload);
    }
}
