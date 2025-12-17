package org.todaybook.bookpreprocessingworker.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
@EnableConfigurationProperties(AppKafkaProperties.class)
public class KafkaConfig {

    @Bean
    public TopicNames topicNames(AppKafkaProperties props) {
        return new TopicNames(props);
    }

    @Bean
    public CommonErrorHandler dlqErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        // 실패 시 "원본토픽명.DLT"로 메시지를 전송함 (예: book.raw -> book.raw.DLT)
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);

        // 1000ms(1초) 간격으로 2번 재시도 (최초 1회 + 재시도 2회 = 총 3회 시도 후 실패 시 DLQ행)
        FixedBackOff backOff = new FixedBackOff(1000L, 2);

        // 공통 모듈의 로깅 에러 핸들러로 대체하여 관측성을 확보
        return new org.todaybook.bookpreprocessingworker.common.kafka.LoggingErrorHandler(recoverer, backOff);
    }

}
