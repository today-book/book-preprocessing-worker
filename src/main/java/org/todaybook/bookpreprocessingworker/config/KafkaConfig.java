package org.todaybook.bookpreprocessingworker.config;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    /**
     * Kafka 설정 클래스의 인스턴스를 생성하고 전달된 Kafka 설정을 보관한다.
     *
     * @param kafkaProperties 애플리케이션의 Kafka 설정을 제공하는 Spring Boot의 KafkaProperties 객체
     */
    public KafkaConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * Kafka에 메시지를 전송하기 위한 ProducerFactory를 생성한다.
     *
     * 생성된 팩토리는 애플리케이션 설정(spring.kafka.producer 및 bootstrap-servers)으로 구성되며,
     * 키 직렬화가 지정되지 않은 경우 StringSerializer를 기본으로 사용하도록 설정한다.
     *
     * @return String 키와 Object 값을 사용하는 초기화된 ProducerFactory 객체
     */

    @Bean
    public ProducerFactory<String, Object> bookProducerFactory() {
        // application.yml 의 spring.kafka.producer.* / spring.kafka.bootstrap-servers 그대로 가져옴
        Map<String, Object> props = kafkaProperties.buildProducerProperties(null);

        // 혹시라도 기본 값이 안 잡혀 있으면 안전하게 한번 더 명시
        props.putIfAbsent(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // value-serializer 는 yml 에 JsonSerializer 로 이미 넣었으니 생략 가능

        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * 애플리케이션에서 메시지 전송에 사용할 KafkaTemplate 빈을 생성한다.
     *
     * @return String 키와 Object 값을 전송하는 KafkaTemplate 인스턴스
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        // 여기서 등록되는 KafkaTemplate<String, Object> 를
        // BookPreprocessingService 에서 주입해서 사용하게 됨
        return new KafkaTemplate<>(bookProducerFactory());
    }

    /**
     * Listener에서 사용될 Kafka 소비자용 ConsumerFactory를 생성한다.
     *
     * 생성된 팩토리는 application 설정에서 빌드한 consumer 프로퍼티를 사용하며,
     * 키와 값의 역직렬화기가 설정되지 않은 경우 기본으로 `StringDeserializer`를 사용하도록 보장한다.
     *
     * @return ConsumerFactory<Object, Object> — 기본 설정과 함께 초기화된 DefaultKafkaConsumerFactory 인스턴스
     */

    @Bean
    public ConsumerFactory<Object, Object> bookConsumerFactory() {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Kafka 리스너에서 사용할 ConcurrentKafkaListenerContainerFactory를 생성하고 반환한다.
     *
     * @return 구성된 ConcurrentKafkaListenerContainerFactory<Object, Object> 인스턴스
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
        ConcurrentKafkaListenerContainerFactoryConfigurer configurer
    ) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, bookConsumerFactory());
        return factory;
    }
}