package org.todaybook.bookpreprocessingworker.application.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.todaybook.bookpreprocessingworker.application.dto.BookConsumeMessage;

@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    topics = {"book.raw", "csv-book.raw", "book.parsed"},
    brokerProperties = {"listeners=PLAINTEXT://localhost:0", "port=0"}
)
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.kafka.consumer.group-id=embedded-book-preprocessor",
    "app.kafka.input-topic=book.raw",
    "app.kafka.output-topic=book.parsed"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class EmbeddedKafkaBookFlowTest {

    private static final String INPUT_TOPIC = "book.raw";
    private static final String OUTPUT_TOPIC = "book.parsed";

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private ObjectMapper objectMapper;

    private Consumer<String, String> outputConsumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("embedded-output-consumer", "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        outputConsumer = new KafkaConsumer<>(consumerProps);
        embeddedKafka.consumeFromAnEmbeddedTopic(outputConsumer, OUTPUT_TOPIC);
    }

    @AfterEach
    void tearDown() {
        if (outputConsumer != null) {
            outputConsumer.close();
        }
    }

    @Test
    void consumeAndProduce_endToEnd() throws Exception {
        String payload = """
            {
              "title": "<b>Sample Book</b>",
              "link": "http://example.com/book",
              "image": "http://example.com/book.jpg",
              "author": "Test Author",
              "price": "15000",
              "discount": "12000",
              "publisher": "Test Pub",
              "pubdate": "20240102",
              "isbn": "9781234567890 123456789X",
              "description": "Sample description for embedded test"
            }
            """;

        kafkaTemplate.send(INPUT_TOPIC, "9781234567890", payload).get();
        kafkaTemplate.flush();

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(outputConsumer, OUTPUT_TOPIC);
            BookConsumeMessage processed = objectMapper.readValue(record.value(), BookConsumeMessage.class);

            assertThat(processed.isbn()).isEqualTo("9781234567890");
            assertThat(processed.title()).isEqualTo("Sample Book");
            assertThat(processed.author()).isEqualTo("Test Author");
            assertThat(processed.description()).contains("embedded test");
        });
    }
}
