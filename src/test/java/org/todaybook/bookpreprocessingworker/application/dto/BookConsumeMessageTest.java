package org.todaybook.bookpreprocessingworker.application.dto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("BookConsumeMessage Record Tests")
class BookConsumeMessageTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Nested
    @DisplayName("Record Creation Tests")
    class RecordCreationTests {

        @Test
        @DisplayName("Given_AllFields_When_CreateRecord_Then_AllFieldsAccessible")
        void givenAllFields_whenCreateRecord_thenAllFieldsAccessible() {
            // given & when
            BookConsumeMessage message = new BookConsumeMessage(
                "9788966262281",
                "이펙티브 자바",
                List.of("컴퓨터", "프로그래밍"),
                "자바 개발자를 위한 필독서",
                "조슈아 블로크",
                "인사이트",
                LocalDate.of(2018, 11, 1),
                "https://image.com/book.jpg"
            );

            // then
            assertThat(message.isbn()).isEqualTo("9788966262281");
            assertThat(message.title()).isEqualTo("이펙티브 자바");
            assertThat(message.categories()).containsExactly("컴퓨터", "프로그래밍");
            assertThat(message.description()).isEqualTo("자바 개발자를 위한 필독서");
            assertThat(message.author()).isEqualTo("조슈아 블로크");
            assertThat(message.publisher()).isEqualTo("인사이트");
            assertThat(message.publishedAt()).isEqualTo(LocalDate.of(2018, 11, 1));
            assertThat(message.thumbnail()).isEqualTo("https://image.com/book.jpg");
        }

        @Test
        @DisplayName("Given_EmptyCategories_When_CreateRecord_Then_CategoriesListIsEmpty")
        void givenEmptyCategories_whenCreateRecord_thenCategoriesListIsEmpty() {
            // given & when
            BookConsumeMessage message = new BookConsumeMessage(
                "9788966262281",
                "이펙티브 자바",
                Collections.emptyList(),
                "설명",
                "저자",
                "출판사",
                LocalDate.now(),
                "https://image.com/book.jpg"
            );

            // then
            assertThat(message.categories()).isEmpty();
        }

        @Test
        @DisplayName("Given_NullPublishedAt_When_CreateRecord_Then_NullIsPreserved")
        void givenNullPublishedAt_whenCreateRecord_thenNullIsPreserved() {
            // given & when
            BookConsumeMessage message = new BookConsumeMessage(
                "9788966262281",
                "이펙티브 자바",
                Collections.emptyList(),
                "설명",
                "저자",
                "출판사",
                null,  // null date
                "https://image.com/book.jpg"
            );

            // then
            assertThat(message.publishedAt()).isNull();
        }

        @Test
        @DisplayName("Given_AllNullFields_When_CreateRecord_Then_NoException")
        void givenAllNullFields_whenCreateRecord_thenNoException() {
            // when & then
            assertThatCode(() -> new BookConsumeMessage(
                null, null, null, null, null, null, null, null
            )).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("JSON Serialization Tests")
    class JsonSerializationTests {

        @Test
        @DisplayName("Given_ValidRecord_When_Serialize_Then_ProducesValidJson")
        void givenValidRecord_whenSerialize_thenProducesValidJson() throws Exception {
            // given
            BookConsumeMessage message = new BookConsumeMessage(
                "9788966262281",
                "이펙티브 자바",
                Collections.emptyList(),
                "자바 개발자를 위한 필독서",
                "조슈아 블로크",
                "인사이트",
                LocalDate.of(2018, 11, 1),
                "https://image.com/book.jpg"
            );

            // when
            String json = objectMapper.writeValueAsString(message);

            // then
            assertThat(json).contains("\"isbn\":\"9788966262281\"");
            assertThat(json).contains("\"title\":\"이펙티브 자바\"");
            assertThat(json).contains("\"categories\":[]");
            assertThat(json).contains("\"author\":\"조슈아 블로크\"");
        }

        @Test
        @DisplayName("Given_RecordWithNullDate_When_Serialize_Then_DateIsNull")
        void givenRecordWithNullDate_whenSerialize_thenDateIsNull() throws Exception {
            // given
            BookConsumeMessage message = new BookConsumeMessage(
                "9788966262281",
                "테스트",
                Collections.emptyList(),
                "설명",
                "저자",
                "출판사",
                null,
                "https://image.com/book.jpg"
            );

            // when
            String json = objectMapper.writeValueAsString(message);

            // then
            assertThat(json).contains("\"publishedAt\":null");
        }

        @Test
        @DisplayName("Given_RecordWithCategories_When_Serialize_Then_CategoriesArrayIncluded")
        void givenRecordWithCategories_whenSerialize_thenCategoriesArrayIncluded() throws Exception {
            // given
            BookConsumeMessage message = new BookConsumeMessage(
                "9788966262281",
                "테스트",
                List.of("컴퓨터", "IT", "프로그래밍"),
                "설명",
                "저자",
                "출판사",
                LocalDate.of(2023, 1, 1),
                "https://image.com/book.jpg"
            );

            // when
            String json = objectMapper.writeValueAsString(message);

            // then
            assertThat(json).contains("\"categories\":[\"컴퓨터\",\"IT\",\"프로그래밍\"]");
        }
    }

    @Nested
    @DisplayName("JSON Deserialization Tests")
    class JsonDeserializationTests {

        @Test
        @DisplayName("Given_ValidJson_When_Deserialize_Then_RecordIsPopulated")
        void givenValidJson_whenDeserialize_thenRecordIsPopulated() throws Exception {
            // given
            String json = """
                {
                    "isbn": "9788966262281",
                    "title": "이펙티브 자바",
                    "categories": [],
                    "description": "자바 개발자를 위한 필독서",
                    "author": "조슈아 블로크",
                    "publisher": "인사이트",
                    "publishedAt": [2018, 11, 1],
                    "thumbnail": "https://image.com/book.jpg"
                }
                """;

            // when
            BookConsumeMessage message = objectMapper.readValue(json, BookConsumeMessage.class);

            // then
            assertThat(message.isbn()).isEqualTo("9788966262281");
            assertThat(message.title()).isEqualTo("이펙티브 자바");
            assertThat(message.publishedAt()).isEqualTo(LocalDate.of(2018, 11, 1));
        }

        @Test
        @DisplayName("Given_JsonWithNullDate_When_Deserialize_Then_DateIsNull")
        void givenJsonWithNullDate_whenDeserialize_thenDateIsNull() throws Exception {
            // given
            String json = """
                {
                    "isbn": "9788966262281",
                    "title": "테스트",
                    "categories": [],
                    "description": "설명",
                    "author": "저자",
                    "publisher": "출판사",
                    "publishedAt": null,
                    "thumbnail": "https://image.com/book.jpg"
                }
                """;

            // when
            BookConsumeMessage message = objectMapper.readValue(json, BookConsumeMessage.class);

            // then
            assertThat(message.publishedAt()).isNull();
        }

        @Test
        @DisplayName("Given_JsonWithCategories_When_Deserialize_Then_CategoriesListPopulated")
        void givenJsonWithCategories_whenDeserialize_thenCategoriesListPopulated() throws Exception {
            // given
            String json = """
                {
                    "isbn": "9788966262281",
                    "title": "테스트",
                    "categories": ["컴퓨터", "프로그래밍", "자바"],
                    "description": "설명",
                    "author": "저자",
                    "publisher": "출판사",
                    "publishedAt": [2023, 1, 1],
                    "thumbnail": "https://image.com/book.jpg"
                }
                """;

            // when
            BookConsumeMessage message = objectMapper.readValue(json, BookConsumeMessage.class);

            // then
            assertThat(message.categories()).hasSize(3);
            assertThat(message.categories()).containsExactly("컴퓨터", "프로그래밍", "자바");
        }
    }

    @Nested
    @DisplayName("Round-Trip Serialization Tests")
    class RoundTripTests {

        @Test
        @DisplayName("Given_ValidRecord_When_SerializeAndDeserialize_Then_RecordIsEqual")
        void givenValidRecord_whenSerializeAndDeserialize_thenRecordIsEqual() throws Exception {
            // given
            BookConsumeMessage original = new BookConsumeMessage(
                "9788966262281",
                "이펙티브 자바",
                List.of("컴퓨터", "프로그래밍"),
                "자바 개발자를 위한 필독서",
                "조슈아 블로크",
                "인사이트",
                LocalDate.of(2018, 11, 1),
                "https://image.com/book.jpg"
            );

            // when
            String json = objectMapper.writeValueAsString(original);
            BookConsumeMessage deserialized = objectMapper.readValue(json, BookConsumeMessage.class);

            // then
            assertThat(deserialized).isEqualTo(original);
        }

        @Test
        @DisplayName("Given_RecordWithNullDate_When_RoundTrip_Then_NullPreserved")
        void givenRecordWithNullDate_whenRoundTrip_thenNullPreserved() throws Exception {
            // given
            BookConsumeMessage original = new BookConsumeMessage(
                "9788966262281",
                "테스트",
                Collections.emptyList(),
                "설명",
                "저자",
                "출판사",
                null,
                "https://image.com/book.jpg"
            );

            // when
            String json = objectMapper.writeValueAsString(original);
            BookConsumeMessage deserialized = objectMapper.readValue(json, BookConsumeMessage.class);

            // then
            assertThat(deserialized.publishedAt()).isNull();
            assertThat(deserialized).isEqualTo(original);
        }
    }

    @Nested
    @DisplayName("Record Equality Tests")
    class EqualityTests {

        @Test
        @DisplayName("Given_TwoIdenticalRecords_When_Compare_Then_AreEqual")
        void givenTwoIdenticalRecords_whenCompare_thenAreEqual() {
            // given
            LocalDate date = LocalDate.of(2023, 1, 1);
            List<String> categories = List.of("카테고리");

            BookConsumeMessage message1 = new BookConsumeMessage(
                "isbn", "title", categories, "desc", "author", "pub", date, "thumb"
            );
            BookConsumeMessage message2 = new BookConsumeMessage(
                "isbn", "title", categories, "desc", "author", "pub", date, "thumb"
            );

            // then
            assertThat(message1).isEqualTo(message2);
            assertThat(message1.hashCode()).isEqualTo(message2.hashCode());
        }

        @Test
        @DisplayName("Given_TwoDifferentRecords_When_Compare_Then_AreNotEqual")
        void givenTwoDifferentRecords_whenCompare_thenAreNotEqual() {
            // given
            BookConsumeMessage message1 = new BookConsumeMessage(
                "isbn1", "title1", Collections.emptyList(), "desc", "author", "pub", LocalDate.now(), "thumb"
            );
            BookConsumeMessage message2 = new BookConsumeMessage(
                "isbn2", "title2", Collections.emptyList(), "desc", "author", "pub", LocalDate.now(), "thumb"
            );

            // then
            assertThat(message1).isNotEqualTo(message2);
        }
    }

    @Nested
    @DisplayName("Categories Immutability Tests")
    class CategoriesImmutabilityTests {

        @Test
        @DisplayName("Given_RecordWithCategories_When_GetCategories_Then_ReturnsListReference")
        void givenRecordWithCategories_whenGetCategories_thenReturnsListReference() {
            // given
            List<String> categories = Arrays.asList("A", "B", "C");
            BookConsumeMessage message = new BookConsumeMessage(
                "isbn", "title", categories, "desc", "author", "pub", LocalDate.now(), "thumb"
            );

            // when
            List<String> retrieved = message.categories();

            // then
            assertThat(retrieved).containsExactly("A", "B", "C");
        }
    }

    @Nested
    @DisplayName("Korean Content Tests")
    class KoreanContentTests {

        @Test
        @DisplayName("Given_KoreanContent_When_SerializeAndDeserialize_Then_PreservesKorean")
        void givenKoreanContent_whenSerializeAndDeserialize_thenPreservesKorean() throws Exception {
            // given
            BookConsumeMessage original = new BookConsumeMessage(
                "9788998139766",
                "객체지향의 사실과 오해",
                List.of("컴퓨터/IT", "프로그래밍"),
                "역할, 책임, 협력 관점에서 본 객체지향",
                "조영호",
                "위키북스",
                LocalDate.of(2015, 6, 17),
                "https://image.com/oop.jpg"
            );

            // when
            String json = objectMapper.writeValueAsString(original);
            BookConsumeMessage deserialized = objectMapper.readValue(json, BookConsumeMessage.class);

            // then
            assertThat(deserialized.title()).isEqualTo("객체지향의 사실과 오해");
            assertThat(deserialized.author()).isEqualTo("조영호");
            assertThat(deserialized.description()).contains("역할");
        }
    }
}
