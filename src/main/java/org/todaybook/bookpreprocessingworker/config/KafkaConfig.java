package org.todaybook.bookpreprocessingworker.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;
import org.todaybook.bookpreprocessingworker.domain.model.Book;

@Configuration
@EnableKafka
@EnableConfigurationProperties(AppKafkaProperties.class)
public class KafkaConfig {

    private static final String NAVER_DTO_PACKAGE =
        "org.todaybook.bookpreprocessingworker.application.dto";

    @Bean
    public TopicNames topicNames(AppKafkaProperties props) {
        return new TopicNames(props);
    }

    @Bean
    public ProducerFactory<String, Book> bookProducerFactory(
        KafkaProperties kafkaProperties,
        ObjectMapper objectMapper
    ) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties(null);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.remove(JsonSerializer.ADD_TYPE_INFO_HEADERS);
        JsonSerializer<Book> valueSerializer = new JsonSerializer<>(configuredObjectMapper(objectMapper));
        valueSerializer.setAddTypeInfo(false);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), valueSerializer);
    }

    @Bean
    public KafkaTemplate<String, Book> bookKafkaTemplate(ProducerFactory<String, Book> bookProducerFactory) {
        return new KafkaTemplate<>(bookProducerFactory);
    }

    @Bean
    public ProducerFactory<String, Object> dlqProducerFactory(
        KafkaProperties kafkaProperties,
        ObjectMapper objectMapper
    ) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties(null);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.remove(JsonSerializer.ADD_TYPE_INFO_HEADERS);
        JsonSerializer<Object> valueSerializer = new JsonSerializer<>(configuredObjectMapper(objectMapper));
        valueSerializer.setAddTypeInfo(false);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), valueSerializer);
    }

    @Bean
    public KafkaTemplate<String, Object> dlqKafkaTemplate(ProducerFactory<String, Object> dlqProducerFactory) {
        return new KafkaTemplate<>(dlqProducerFactory);
    }

    @Bean
    public ConsumerFactory<String, String> csvConsumerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(
            props,
            new StringDeserializer(),
            new StringDeserializer()
        );
    }

    @Bean
    public ConsumerFactory<String, NaverBookItem> jsonConsumerFactory(
        KafkaProperties kafkaProperties,
        ObjectMapper kafkaConsumerObjectMapper
    ) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);

        JsonDeserializer<NaverBookItem> deserializer =
            new JsonDeserializer<>(NaverBookItem.class, kafkaConsumerObjectMapper, false);
        deserializer.addTrustedPackages(NAVER_DTO_PACKAGE);

        return new DefaultKafkaConsumerFactory<>(
            props,
            new StringDeserializer(),
            deserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> csvKafkaListenerContainerFactory(
        ConsumerFactory<String, String> csvConsumerFactory,
        CommonErrorHandler dlqErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(csvConsumerFactory);
        factory.setCommonErrorHandler(dlqErrorHandler);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, NaverBookItem> jsonKafkaListenerContainerFactory(
        ConsumerFactory<String, NaverBookItem> jsonConsumerFactory,
        CommonErrorHandler dlqErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, NaverBookItem> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(jsonConsumerFactory);
        factory.setCommonErrorHandler(dlqErrorHandler);
        return factory;
    }

    @Bean
    public CommonErrorHandler dlqErrorHandler(KafkaTemplate<String, Object> dlqKafkaTemplate) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(dlqKafkaTemplate);
        FixedBackOff backOff = new FixedBackOff(1000L, 2);
        return new org.todaybook.common.kafka.LoggingErrorHandler(recoverer, backOff);
    }

    private ObjectMapper configuredObjectMapper(ObjectMapper baseMapper) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

}
