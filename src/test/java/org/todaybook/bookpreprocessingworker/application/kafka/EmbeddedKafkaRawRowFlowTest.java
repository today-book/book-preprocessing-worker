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
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.todaybook.bookpreprocessingworker.config.TopicNames;
import org.todaybook.bookpreprocessingworker.domain.model.Book;

@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    topics = {"book.raw.naver", "book.raw.csv", "book.parsed"},
    brokerProperties = {"listeners=PLAINTEXT://localhost:0", "port=0"}
)
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.kafka.consumer.group-id=csv-embedded-book-preprocessor",
    "app.kafka.input-topic=book.raw.naver",
    "app.kafka.csv-input-topic=book.raw.csv",
    "app.kafka.output-topic=book.parsed"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class EmbeddedKafkaRawRowFlowTest {

    private KafkaTemplate<String, String> inputKafkaTemplate;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private TopicNames topicNames;

    private Consumer<String, String> outputConsumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        inputKafkaTemplate = new KafkaTemplate<>(
            new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new StringSerializer())
        );

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("csv-embedded-output-consumer", "false", embeddedKafka);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        outputConsumer = new KafkaConsumer<>(consumerProps);
        embeddedKafka.consumeFromAnEmbeddedTopic(outputConsumer, topicNames.outputTopic());
    }

    @AfterEach
    void tearDown() {
        if (outputConsumer != null) {
            outputConsumer.close();
        }
    }

    @Test
    void consumeRawRowAndProduce_endToEnd() throws Exception {
        String rawPayload = "\"115982\",\"9780761921585\",\"cloth\",\"Designing for learning\",\"George W. Gagnon\",\"Corwin Press\",\"\",\"\",\"121081\",\"http://image.aladin.co.kr/cover.jpg\",\"A great book about education and learning practices.\",\"\",\"slug\",\"\",\"2000-12-29\",\"Y\",\"Y\",\"0761921583 (cloth)\"";

        inputKafkaTemplate.send(topicNames.csvInputTopic(), "9780761921585", rawPayload).get();
        inputKafkaTemplate.flush();

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(outputConsumer, topicNames.outputTopic());
            Book processed = objectMapper.readValue(record.value(), Book.class);

            assertThat(processed.isbn()).isEqualTo("9780761921585");
            assertThat(processed.title()).isEqualTo("Designing for learning");
            assertThat(processed.author()).isEqualTo("George W. Gagnon");
            assertThat(processed.publisher()).isEqualTo("Corwin Press");
            assertThat(processed.description()).isEqualTo("A great book about education and learning practices.");
        });
    }
}
