package org.todaybook.bookpreprocessingworker.application.kafka.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@ActiveProfiles("local")
@Tag("docker-kafka")
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=localhost:9092",
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.kafka.consumer.group-id=docker-book-preprocessor",
    "spring.kafka.properties.security.protocol=PLAINTEXT",
    "app.kafka.input-topic=book.raw",
    "app.kafka.output-topic=book.parsed",
    "KAFKA_BOOTSTRAP_SERVERS=localhost:9092"
})
class DockerKafkaIntegrationTest {

    private static final String INPUT_TOPIC = "book.raw";
    private static final String OUTPUT_TOPIC = "book.parsed";

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private KafkaConsumer<String, String> dockerConsumer;

    /* -------------------------------------------------
     * Test lifecycle
     * ------------------------------------------------- */

    @BeforeEach
    void setUp() {
        Assumptions.assumeTrue(
            isKafkaReachable(),
            "Kafka is not reachable at localhost:9092. Is docker-compose up?"
        );

        ensureTopicsExist();
        dockerConsumer = createConsumer();
        waitForAssignmentAndSeekToBeginning(dockerConsumer);
    }

    @AfterEach
    void tearDown() {
        if (dockerConsumer != null) {
            dockerConsumer.close();
        }
    }

    /* -------------------------------------------------
     * Test
     * ------------------------------------------------- */

    @Test
    void messageTravelsThroughRealKafka() throws Exception {
        // given
        String isbn = "9780987654321";
        String payload = """
            {
              "title": "Real Kafka Book",
              "link": "http://example.com/real-book",
              "image": "http://example.com/real-book.jpg",
              "author": "Integration Author",
              "price": "19000",
              "discount": "15000",
              "publisher": "Integration Pub",
              "pubdate": "20240201",
              "isbn": "%s",
              "description": "Message used for docker-based Kafka connectivity test"
            }
            """.formatted(isbn);

        // when
        kafkaTemplate.send(INPUT_TOPIC, isbn, payload).get();
        kafkaTemplate.flush();

        // then
        await()
            .atMost(Duration.ofSeconds(40))
            .untilAsserted(() -> {
                ConsumerRecords<String, String> records =
                    KafkaTestUtils.getRecords(dockerConsumer, Duration.ofSeconds(3));

                assertThat(records.records(OUTPUT_TOPIC))
                    .anySatisfy(record ->
                        assertThat(record.value()).contains(isbn)
                    );
            });
    }

    /* -------------------------------------------------
     * Kafka helpers
     * ------------------------------------------------- */

    private KafkaConsumer<String, String> createConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(
            ConsumerConfig.GROUP_ID_CONFIG,
            "docker-connectivity-consumer-" + System.currentTimeMillis()
        );
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));
        return consumer;
    }

    private void waitForAssignmentAndSeekToBeginning(KafkaConsumer<String, String> consumer) {

        await()
            .atMost(15, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                consumer.poll(Duration.ofMillis(500));
                assertThat(consumer.assignment()).isNotEmpty();
            });

        consumer.seekToBeginning(consumer.assignment());
    }


    private void ensureTopicsExist() {
        Map<String, Object> adminProps =
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            var existingTopics =
                adminClient.listTopics().names().get(5, TimeUnit.SECONDS);

            List<NewTopic> topicsToCreate = List.of(INPUT_TOPIC, OUTPUT_TOPIC)
                .stream()
                .filter(topic -> !existingTopics.contains(topic))
                .map(topic -> new NewTopic(topic, 1, (short) 1))
                .toList();

            if (!topicsToCreate.isEmpty()) {
                adminClient.createTopics(topicsToCreate)
                    .all()
                    .get(10, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to ensure Kafka topics exist", e);
        }
    }

    private boolean isKafkaReachable() {
        Map<String, Object> adminProps =
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            adminClient.listTopics().names().get(3, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
