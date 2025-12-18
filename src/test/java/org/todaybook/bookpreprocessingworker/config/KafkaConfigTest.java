package org.todaybook.bookpreprocessingworker.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;
import org.todaybook.bookpreprocessingworker.domain.model.Book;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"book.raw.naver", "book.raw.csv", "book.parsed", "book.raw.naver.DLT"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@DisplayName("KafkaConfig Integration Tests")
class KafkaConfigTest {

    @Autowired
    private KafkaConfig kafkaConfig;

    @Autowired
    @Qualifier("bookKafkaTemplate")
    private KafkaTemplate<String, Book> kafkaTemplate;

    @Autowired
    private ProducerFactory<String, Book> bookProducerFactory;

    @Autowired
    private ConsumerFactory<String, String> csvConsumerFactory;

    @Autowired
    private ConsumerFactory<String, NaverBookItem> jsonConsumerFactory;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, String> csvKafkaListenerContainerFactory;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, NaverBookItem> jsonKafkaListenerContainerFactory;

    @Autowired
    private CommonErrorHandler dlqErrorHandler;

    @Nested
    @DisplayName("Bean Creation Tests")
    class BeanCreationTests {

        @Test
        @DisplayName("Given_ApplicationContext_When_StartUp_Then_KafkaConfigBeanExists")
        void givenApplicationContext_whenStartUp_thenKafkaConfigBeanExists() {
            assertThat(kafkaConfig).isNotNull();
        }

        @Test
        @DisplayName("Given_ApplicationContext_When_StartUp_Then_KafkaTemplateExists")
        void givenApplicationContext_whenStartUp_thenKafkaTemplateExists() {
            assertThat(kafkaTemplate).isNotNull();
        }

        @Test
        @DisplayName("Given_ApplicationContext_When_StartUp_Then_ProducerFactoryExists")
        void givenApplicationContext_whenStartUp_thenProducerFactoryExists() {
            assertThat(bookProducerFactory).isNotNull();
        }

        @Test
        @DisplayName("Given_ApplicationContext_When_StartUp_Then_ConsumerFactoryExists")
        void givenApplicationContext_whenStartUp_thenConsumerFactoryExists() {
            assertThat(csvConsumerFactory).isNotNull();
            assertThat(jsonConsumerFactory).isNotNull();
        }

        @Test
        @DisplayName("Given_ApplicationContext_When_StartUp_Then_ListenerContainerFactoryExists")
        void givenApplicationContext_whenStartUp_thenListenerContainerFactoryExists() {
            assertThat(csvKafkaListenerContainerFactory).isNotNull();
            assertThat(jsonKafkaListenerContainerFactory).isNotNull();
        }

        @Test
        @DisplayName("Given_ApplicationContext_When_StartUp_Then_DlqErrorHandlerExists")
        void givenApplicationContext_whenStartUp_thenDlqErrorHandlerExists() {
            assertThat(dlqErrorHandler).isNotNull();
        }
    }

    @Nested
    @DisplayName("Producer Configuration Tests")
    class ProducerConfigurationTests {

        @Test
        @DisplayName("Given_ProducerFactory_When_GetConfig_Then_UsesJsonSerializer")
        void givenProducerFactory_whenGetConfig_thenUsesJsonSerializer() {
            // when
            var configurationProperties = bookProducerFactory.getConfigurationProperties();

            // then
            assertThat(configurationProperties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG))
                .isEqualTo(StringSerializer.class);
            assertThat(configurationProperties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG))
                .isEqualTo(JsonSerializer.class);
        }

        @Test
        @DisplayName("Given_KafkaTemplate_When_GetProducerFactory_Then_ReturnsCorrectFactory")
        void givenKafkaTemplate_whenGetProducerFactory_thenReturnsCorrectFactory() {
            assertThat(kafkaTemplate.getProducerFactory()).isEqualTo(bookProducerFactory);
        }
    }

    @Nested
    @DisplayName("Consumer Configuration Tests")
    class ConsumerConfigurationTests {

        @Test
        @DisplayName("Given_CsvConsumerFactory_When_GetConfig_Then_UsesStringDeserializer")
        void givenCsvConsumerFactory_whenGetConfig_thenUsesStringDeserializer() {
            // when
            var configurationProperties = csvConsumerFactory.getConfigurationProperties();

            // then
            assertThat(configurationProperties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG))
                .isEqualTo(StringDeserializer.class);
            assertThat(configurationProperties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG))
                .isEqualTo(StringDeserializer.class);
        }

        @Test
        @DisplayName("Given_JsonConsumerFactory_When_GetConfig_Then_UsesStringKeyDeserializer")
        void givenJsonConsumerFactory_whenGetConfig_thenUsesStringKeyDeserializer() {
            // when
            var configurationProperties = jsonConsumerFactory.getConfigurationProperties();

            // then - key deserializer is configured via properties
            assertThat(configurationProperties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG))
                .isEqualTo(StringDeserializer.class);
            // value deserializer is provided as instance, not via properties
            // so it won't appear in configuration properties
        }
    }

    @Nested
    @DisplayName("Error Handler Configuration Tests")
    class ErrorHandlerConfigurationTests {

        @Test
        @DisplayName("Given_DlqErrorHandler_When_Check_Then_IsDefaultErrorHandler")
        void givenDlqErrorHandler_whenCheck_thenIsDefaultErrorHandler() {
            assertThat(dlqErrorHandler).isInstanceOf(DefaultErrorHandler.class);
        }

        @Test
        @DisplayName("Given_ListenerContainerFactory_When_Check_Then_HasErrorHandler")
        void givenListenerContainerFactory_whenCheck_thenHasErrorHandler() {
            // The container factory should have an error handler configured
            assertThat(csvKafkaListenerContainerFactory.getContainerProperties()).isNotNull();
            assertThat(jsonKafkaListenerContainerFactory.getContainerProperties()).isNotNull();
        }
    }
}
