package org.todaybook.bookpreprocessingworker.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"book.raw", "book.parsed", "csv-book.raw"})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ActiveProfiles("test")
@DisplayName("AppKafkaProperties Integration Tests")
class AppKafkaPropertiesTest {

    @Autowired
    private AppKafkaProperties appKafkaProperties;

    @Nested
    @DisplayName("Property Binding Tests")
    class PropertyBindingTests {

        @Test
        @DisplayName("Given_ApplicationContext_When_StartUp_Then_PropertiesBeanExists")
        void givenApplicationContext_whenStartUp_thenPropertiesBeanExists() {
            assertThat(appKafkaProperties).isNotNull();
        }

        @Test
        @DisplayName("Given_ApplicationYml_When_Loaded_Then_InputTopicIsBound")
        void givenApplicationYml_whenLoaded_thenInputTopicIsBound() {
            assertThat(appKafkaProperties.getInputTopic()).isEqualTo("book.raw");
        }

        @Test
        @DisplayName("Given_ApplicationYml_When_Loaded_Then_CsvInputTopicIsBound")
        void givenApplicationYml_whenLoaded_thenCsvInputTopicIsBound() {
            assertThat(appKafkaProperties.getCsvInputTopic()).isEqualTo("csv-book.raw");
        }

        @Test
        @DisplayName("Given_ApplicationYml_When_Loaded_Then_OutputTopicIsBound")
        void givenApplicationYml_whenLoaded_thenOutputTopicIsBound() {
            assertThat(appKafkaProperties.getOutputTopic()).isEqualTo("book.parsed");
        }
    }

    @Nested
    @DisplayName("Getter/Setter Tests")
    class GetterSetterTests {

        @Test
        @DisplayName("Given_NewInputTopic_When_SetInputTopic_Then_GetInputTopicReturnsNewValue")
        void givenNewInputTopic_whenSetInputTopic_thenGetInputTopicReturnsNewValue() {
            // given
            AppKafkaProperties properties = new AppKafkaProperties();
            String newTopic = "new.input.topic";

            // when
            properties.setInputTopic(newTopic);

            // then
            assertThat(properties.getInputTopic()).isEqualTo(newTopic);
        }

        @Test
        @DisplayName("Given_NewOutputTopic_When_SetOutputTopic_Then_GetOutputTopicReturnsNewValue")
        void givenNewOutputTopic_whenSetOutputTopic_thenGetOutputTopicReturnsNewValue() {
            // given
            AppKafkaProperties properties = new AppKafkaProperties();
            String newTopic = "new.output.topic";

            // when
            properties.setOutputTopic(newTopic);

            // then
            assertThat(properties.getOutputTopic()).isEqualTo(newTopic);
        }

        @Test
        @DisplayName("Given_NewProperties_When_NotSet_Then_ReturnsNull")
        void givenNewProperties_whenNotSet_thenReturnsNull() {
            // given
            AppKafkaProperties properties = new AppKafkaProperties();

            // then
            assertThat(properties.getInputTopic()).isNull();
            assertThat(properties.getOutputTopic()).isNull();
        }
    }

    @Nested
    @DisplayName("Edge Case Tests")
    class EdgeCaseTests {

        @Test
        @DisplayName("Given_EmptyString_When_SetInputTopic_Then_AcceptsEmptyString")
        void givenEmptyString_whenSetInputTopic_thenAcceptsEmptyString() {
            // given
            AppKafkaProperties properties = new AppKafkaProperties();

            // when
            properties.setInputTopic("");

            // then
            assertThat(properties.getInputTopic()).isEmpty();
        }

        @Test
        @DisplayName("Given_NullValue_When_SetInputTopic_Then_AcceptsNull")
        void givenNullValue_whenSetInputTopic_thenAcceptsNull() {
            // given
            AppKafkaProperties properties = new AppKafkaProperties();
            properties.setInputTopic("some.topic");

            // when
            properties.setInputTopic(null);

            // then
            assertThat(properties.getInputTopic()).isNull();
        }

        @Test
        @DisplayName("Given_TopicWithSpecialCharacters_When_Set_Then_PreservesCharacters")
        void givenTopicWithSpecialCharacters_whenSet_thenPreservesCharacters() {
            // given
            AppKafkaProperties properties = new AppKafkaProperties();
            String specialTopic = "my-topic.with_special-chars.v1";

            // when
            properties.setInputTopic(specialTopic);

            // then
            assertThat(properties.getInputTopic()).isEqualTo(specialTopic);
        }
    }
}
