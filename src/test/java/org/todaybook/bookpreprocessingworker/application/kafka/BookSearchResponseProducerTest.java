package org.todaybook.bookpreprocessingworker.application.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(
    partitions = 1,
    topics = {"book.raw", "csv-book.raw", "book.parsed", "book.raw.DLT"},
    brokerProperties = {
        "listeners=PLAINTEXT://localhost:0",
        "port=0"
    }
)
@SpringBootTest(properties = {
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "app.kafka.input-topic=book.raw"
})
@ActiveProfiles("test")
@DisplayName("BookSearchResponse Producer Integration Tests")
class BookSearchResponseProducerTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.input-topic}")
    private String topic;

    @Nested
    @DisplayName("Kafka Producer Tests")
    class KafkaProducerTests {

        @Test
        @DisplayName("Given_ValidSingleItemPayload_When_SendToKafka_Then_SucceedsWithOffset")
        void givenValidSingleItemPayload_whenSendToKafka_thenSucceedsWithOffset()
            throws InterruptedException, ExecutionException, TimeoutException {
            // given
            String jsonPayload = """
                {
                    "title": "이펙티브 자바",
                    "link": "https://search.shopping.naver.com/book/catalog/123",
                    "image": "https://shopping-phinf.pstatic.net/main_123/123.jpg",
                    "author": "조슈아 블로크",
                    "discount": "32400",
                    "publisher": "인사이트",
                    "pubdate": "20181101",
                    "isbn": "9788966262281",
                    "description": "자바 개발자를 위한 필독서"
                }
                """;

            // when
            var result = kafkaTemplate.send(topic, jsonPayload).get(10, TimeUnit.SECONDS);

            // then
            assertThat(result).isNotNull();
            assertThat(result.getRecordMetadata().offset()).isGreaterThanOrEqualTo(0);
            assertThat(result.getRecordMetadata().topic()).isEqualTo(topic);
        }

        @Test
        @DisplayName("Given_RealNaverApiPayload_When_SendToKafka_Then_SucceedsWithOffset")
        void givenRealNaverApiPayload_whenSendToKafka_thenSucceedsWithOffset()
            throws InterruptedException, ExecutionException, TimeoutException {
            // given - realistic single-item Naver API response
            String jsonPayload = """
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
            var result = kafkaTemplate.send(topic, jsonPayload).get(10, TimeUnit.SECONDS);

            // then
            assertThat(result).isNotNull();
            assertThat(result.getRecordMetadata().offset()).isGreaterThanOrEqualTo(0);
        }

        @Test
        @DisplayName("Given_PayloadWithMixedIsbn_When_SendToKafka_Then_Succeeds")
        void givenPayloadWithMixedIsbn_whenSendToKafka_thenSucceeds()
            throws InterruptedException, ExecutionException, TimeoutException {
            // given
            String jsonPayload = """
                {
                    "title": "테스트 책",
                    "isbn": "8966262287 9788966262281",
                    "author": "저자",
                    "description": "설명"
                }
                """;

            // when
            var result = kafkaTemplate.send(topic, jsonPayload).get(10, TimeUnit.SECONDS);

            // then
            assertThat(result).isNotNull();
            assertThat(result.getRecordMetadata().offset()).isGreaterThanOrEqualTo(0);
        }

        @Test
        @DisplayName("Given_PayloadWithHtmlTitle_When_SendToKafka_Then_Succeeds")
        void givenPayloadWithHtmlTitle_whenSendToKafka_thenSucceeds()
            throws InterruptedException, ExecutionException, TimeoutException {
            // given
            String jsonPayload = """
                {
                    "title": "<b>헤드 퍼스트</b> 디자인 패턴",
                    "isbn": "9788979143400",
                    "author": "에릭 프리먼",
                    "publisher": "한빛미디어",
                    "pubdate": "20050901",
                    "description": "디자인 패턴 입문서"
                }
                """;

            // when
            var result = kafkaTemplate.send(topic, jsonPayload).get(10, TimeUnit.SECONDS);

            // then
            assertThat(result).isNotNull();
        }

        @Test
        @DisplayName("Given_MinimalPayload_When_SendToKafka_Then_Succeeds")
        void givenMinimalPayload_whenSendToKafka_thenSucceeds()
            throws InterruptedException, ExecutionException, TimeoutException {
            // given - only essential fields
            String jsonPayload = """
                {
                    "title": "최소 필드 책",
                    "isbn": "9781234567890",
                    "author": "저자",
                    "description": "설명"
                }
                """;

            // when
            var result = kafkaTemplate.send(topic, jsonPayload).get(10, TimeUnit.SECONDS);

            // then
            assertThat(result).isNotNull();
            assertThat(result.getRecordMetadata().offset()).isGreaterThanOrEqualTo(0);
        }

        @Test
        @DisplayName("Given_MultipleMessages_When_SendSequentially_Then_AllSucceed")
        void givenMultipleMessages_whenSendSequentially_thenAllSucceed()
            throws InterruptedException, ExecutionException, TimeoutException {
            // given
            String[] payloads = {
                """
                {"title":"책1","isbn":"9781111111111","author":"저자1","description":"설명1"}
                """,
                """
                {"title":"책2","isbn":"9782222222222","author":"저자2","description":"설명2"}
                """,
                """
                {"title":"책3","isbn":"9783333333333","author":"저자3","description":"설명3"}
                """
            };

            // when & then
            for (int i = 0; i < payloads.length; i++) {
                var result = kafkaTemplate.send(topic, payloads[i]).get(10, TimeUnit.SECONDS);
                assertThat(result).isNotNull();
                assertThat(result.getRecordMetadata().offset()).isGreaterThanOrEqualTo(i);
            }
        }
    }

    @Nested
    @DisplayName("Kafka Template Verification Tests")
    class KafkaTemplateVerificationTests {

        @Test
        @DisplayName("Given_SpringContext_When_GetKafkaTemplate_Then_IsNotNull")
        void givenSpringContext_whenGetKafkaTemplate_thenIsNotNull() {
            assertThat(kafkaTemplate).isNotNull();
        }

        @Test
        @DisplayName("Given_Configuration_When_GetTopic_Then_IsBookRaw")
        void givenConfiguration_whenGetTopic_thenIsBookRaw() {
            assertThat(topic).isEqualTo("book.raw");
        }
    }
}
