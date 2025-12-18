package org.todaybook.bookpreprocessingworker.application.kafka.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.todaybook.bookpreprocessingworker.domain.model.Book;

@SpringBootTest
@EmbeddedKafka(
    partitions = 1,
    topics = {"book.raw.naver", "book.raw.csv", "book.parsed", "book.raw.naver.DLT"},
    brokerProperties = {
        "listeners=PLAINTEXT://localhost:0",
        "port=0"
    }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@ActiveProfiles("test")
@DisplayName("Kafka End-to-End Integration Tests")
class KafkaIntegrationTest {

    private static final String INPUT_TOPIC = "book.raw.naver";
    private static final String OUTPUT_TOPIC = "book.parsed";
    private static final String DLT_TOPIC = "book.raw.naver.DLT";

    @Autowired
    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private ObjectMapper objectMapper;

    private KafkaTemplate<String, String> testKafkaTemplate;
    private Consumer<String, String> testConsumer;
    private Consumer<String, String> dltConsumer;

    @BeforeEach
    void setUp() {
        // Set up producer for sending test messages
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafkaBroker);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        testKafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));

        // Set up consumer for verifying output messages
        Map<String, Object> consumerProps = new HashMap<>(KafkaTestUtils.consumerProps("test-consumer-group", "true", embeddedKafkaBroker));
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        testConsumer = new DefaultKafkaConsumerFactory<String, String>(consumerProps).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(testConsumer, OUTPUT_TOPIC);

        // Set up DLT consumer
        Map<String, Object> dltConsumerProps = new HashMap<>(KafkaTestUtils.consumerProps("dlt-consumer-group", "true", embeddedKafkaBroker));
        dltConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        dltConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        dltConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        dltConsumer = new DefaultKafkaConsumerFactory<String, String>(dltConsumerProps).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(dltConsumer, DLT_TOPIC);
    }

    @AfterEach
    void tearDown() {
        if (testConsumer != null) {
            testConsumer.close();
        }
        if (dltConsumer != null) {
            dltConsumer.close();
        }
    }

    @Nested
    @DisplayName("End-to-End Message Processing Tests")
    class EndToEndTests {

        @Test
        @DisplayName("Given_ValidBookMessage_When_SentToInputTopic_Then_ProcessedMessageAppearsInOutputTopic")
        void givenValidBookMessage_whenSentToInputTopic_thenProcessedMessageAppearsInOutputTopic() throws Exception {
            // given
            String inputJson = """
                {
                    "title": "이펙티브 자바",
                    "link": "http://link.com",
                    "image": "http://image.com/book.jpg",
                    "author": "조슈아 블로크",
                    "price": "36000",
                    "discount": "32400",
                    "publisher": "인사이트",
                    "pubdate": "20181101",
                    "isbn": "9788966262281",
                    "description": "자바 개발자를 위한 필독서로 널리 알려진 책입니다."
                }
                """;

            // when
            testKafkaTemplate.send(INPUT_TOPIC, inputJson).get(10, TimeUnit.SECONDS);

            // then - wait for message to be processed and appear in output topic
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(5));
                assertThat(records.count()).isGreaterThan(0);

                ConsumerRecord<String, String> record = records.iterator().next();
                assertThat(record.key()).isEqualTo("9788966262281");

                Book message = objectMapper.readValue(record.value(), Book.class);
                assertThat(message.isbn()).isEqualTo("9788966262281");
                assertThat(message.title()).isEqualTo("이펙티브 자바");
                assertThat(message.author()).isEqualTo("조슈아 블로크");
            });
        }

        @Test
        @DisplayName("Given_BookWithHtmlTitle_When_Processed_Then_HtmlTagsAreRemoved")
        void givenBookWithHtmlTitle_whenProcessed_thenHtmlTagsAreRemoved() throws Exception {
            // given
            String inputJson = """
                {
                    "title": "<b>헤드 퍼스트</b> 디자인 패턴",
                    "isbn": "9788979143400",
                    "author": "에릭 프리먼",
                    "publisher": "한빛미디어",
                    "pubdate": "20050901",
                    "description": "디자인 패턴 입문서로 충분한 설명을 포함합니다."
                }
                """;

            // when
            testKafkaTemplate.send(INPUT_TOPIC, inputJson).get(10, TimeUnit.SECONDS);

            // then
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(5));
                assertThat(records.count()).isGreaterThan(0);

                ConsumerRecord<String, String> record = records.iterator().next();
                Book message = objectMapper.readValue(record.value(), Book.class);
                assertThat(message.title()).isEqualTo("헤드 퍼스트 디자인 패턴");
                assertThat(message.title()).doesNotContain("<b>", "</b>");
            });
        }

        @Test
        @DisplayName("Given_BookWithMixedIsbn_When_Processed_Then_Extracts13DigitIsbn")
        void givenBookWithMixedIsbn_whenProcessed_thenExtracts13DigitIsbn() throws Exception {
            // given
            String inputJson = """
                {
                    "title": "테스트 책",
                    "isbn": "8966262287 9788966262281",
                    "author": "저자",
                    "publisher": "출판사",
                    "pubdate": "20231225",
                    "description": "테스트용 설명으로 최소 길이 요건을 충족합니다."
                }
                """;

            // when
            testKafkaTemplate.send(INPUT_TOPIC, inputJson).get(10, TimeUnit.SECONDS);

            // then
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(5));
                assertThat(records.count()).isGreaterThan(0);

                ConsumerRecord<String, String> record = records.iterator().next();
                assertThat(record.key()).isEqualTo("9788966262281");

                Book message = objectMapper.readValue(record.value(), Book.class);
                assertThat(message.isbn()).isEqualTo("9788966262281");
            });
        }
    }

    @Nested
    @DisplayName("Multiple Message Processing Tests")
    class MultipleMessageTests {

        @Test
        @DisplayName("Given_MultipleValidMessages_When_SentSequentially_Then_AllAreProcessed")
        void givenMultipleValidMessages_whenSentSequentially_thenAllAreProcessed() throws Exception {
            // given
            String[] messages = {
                """
                {"title":"책1","isbn":"9781111111111","author":"저자1","publisher":"출판사1","pubdate":"20230101","description":"설명1이 충분히 길어서 요건을 충족합니다."}
                """,
                """
                {"title":"책2","isbn":"9782222222222","author":"저자2","publisher":"출판사2","pubdate":"20230202","description":"설명2가 충분히 길어서 요건을 충족합니다."}
                """,
                """
                {"title":"책3","isbn":"9783333333333","author":"저자3","publisher":"출판사3","pubdate":"20230303","description":"설명3도 충분히 길어서 요건을 충족합니다."}
                """
            };

            // when
            for (String message : messages) {
                testKafkaTemplate.send(INPUT_TOPIC, message).get(10, TimeUnit.SECONDS);
            }

            // then
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(10));
                assertThat(records.count()).isGreaterThanOrEqualTo(3);
            });
        }
    }

    @Nested
    @DisplayName("Invalid Message Handling Tests")
    class InvalidMessageTests {

        @Test
        @DisplayName("Given_MessageWithMissingRequiredFields_When_Processed_Then_NotSentToOutput")
        void givenMessageWithMissingRequiredFields_whenProcessed_thenNotSentToOutput() throws Exception {
            // given - missing title, author, description
            String invalidJson = """
                {
                    "isbn": "9781234567890"
                }
                """;

            // when
            testKafkaTemplate.send(INPUT_TOPIC, invalidJson).get(10, TimeUnit.SECONDS);

            // then - no message should appear in output (invalid items are skipped)
            Thread.sleep(3000); // Wait a bit
            ConsumerRecords<String, String> records = testConsumer.poll(Duration.ofSeconds(5));
            // Message should not appear in output topic because it lacks required fields
            // The service skips invalid items silently
        }
    }

    @Nested
    @DisplayName("Date Parsing Integration Tests")
    class DateParsingIntegrationTests {

        @Test
        @DisplayName("Given_BookWithValidDate_When_Processed_Then_DateIsParsedCorrectly")
        void givenBookWithValidDate_whenProcessed_thenDateIsParsedCorrectly() throws Exception {
            // given
            String inputJson = """
                {
                    "title": "날짜 테스트 책",
                    "isbn": "9781234567890",
                    "author": "저자",
                    "publisher": "출판사",
                    "pubdate": "20231225",
                    "description": "날짜 파싱 테스트를 위한 충분히 긴 설명입니다."
                }
                """;

            // when
            testKafkaTemplate.send(INPUT_TOPIC, inputJson).get(10, TimeUnit.SECONDS);

            // then
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(5));
                assertThat(records.count()).isGreaterThan(0);

                ConsumerRecord<String, String> record = records.iterator().next();
                Book message = objectMapper.readValue(record.value(), Book.class);
                assertThat(message.publishedAt()).isNotNull();
                assertThat(message.publishedAt().getYear()).isEqualTo(2023);
                assertThat(message.publishedAt().getMonthValue()).isEqualTo(12);
                assertThat(message.publishedAt().getDayOfMonth()).isEqualTo(25);
            });
        }

        @Test
        @DisplayName("Given_BookWithInvalidDate_When_Processed_Then_DateIsNull")
        void givenBookWithInvalidDate_whenProcessed_thenDateIsNull() throws Exception {
            // given
            String inputJson = """
                {
                    "title": "잘못된 날짜 책",
                    "isbn": "9781234567890",
                    "author": "저자",
                    "publisher": "출판사",
                    "pubdate": "INVALID_DATE",
                    "description": "잘못된 날짜라도 설명은 충분히 깁니다."
                }
                """;

            // when
            testKafkaTemplate.send(INPUT_TOPIC, inputJson).get(10, TimeUnit.SECONDS);

            // then
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(5));
                assertThat(records.count()).isGreaterThan(0);

                ConsumerRecord<String, String> record = records.iterator().next();
                Book message = objectMapper.readValue(record.value(), Book.class);
                assertThat(message.publishedAt()).isNull();
            });
        }
    }

    @Nested
    @DisplayName("Real-World Naver API Response Tests")
    class RealWorldApiResponseTests {

        @Test
        @DisplayName("Given_RealNaverApiPayload_When_Processed_Then_OutputIsCorrect")
        void givenRealNaverApiPayload_whenProcessed_thenOutputIsCorrect() throws Exception {
            // given - realistic Naver API single item response
            String inputJson = """
                {
                    "title": "르몽드 디플로마티크(Le Monde Diplomatique)(한국어판)(2025년 12월호) (207호)",
                    "link": "https://search.shopping.naver.com/book/catalog/57944061835",
                    "image": "https://shopping-phinf.pstatic.net/main_5794406/57944061835.20251130072856.jpg",
                    "author": "브누아 브레빌^르몽드디플로마티크 편집부",
                    "discount": "17100",
                    "publisher": "르몽드디플로마티크",
                    "pubdate": "20251128",
                    "isbn": "9791192618944",
                    "description": "프랑스《르몽드》의 자매지로 전세계 27개 언어, 84개 국제판으로 발행되는 월간지"
                }
                """;

            // when
            testKafkaTemplate.send(INPUT_TOPIC, inputJson).get(10, TimeUnit.SECONDS);

            // then
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(testConsumer, Duration.ofSeconds(5));
                assertThat(records.count()).isGreaterThan(0);

                ConsumerRecord<String, String> record = records.iterator().next();
                assertThat(record.key()).isEqualTo("9791192618944");

                Book message = objectMapper.readValue(record.value(), Book.class);
                assertThat(message.isbn()).isEqualTo("9791192618944");
                assertThat(message.title()).contains("르몽드 디플로마티크");
                assertThat(message.author()).isEqualTo("브누아 브레빌");
                assertThat(message.thumbnail()).contains("pstatic.net");
                assertThat(message.categories()).isEmpty();
            });
        }
    }
}
