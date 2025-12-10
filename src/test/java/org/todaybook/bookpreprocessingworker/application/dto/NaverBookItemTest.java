package org.todaybook.bookpreprocessingworker.application.dto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@DisplayName("NaverBookItem Record Tests")
class NaverBookItemTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
    }

    @Nested
    @DisplayName("Record Creation Tests")
    class RecordCreationTests {

        @Test
        @DisplayName("Given_AllFields_When_CreateRecord_Then_AllFieldsAccessible")
        void givenAllFields_whenCreateRecord_thenAllFieldsAccessible() {
            // given & when
            NaverBookItem item = new NaverBookItem(
                "이펙티브 자바",
                "https://link.com",
                "https://image.com/book.jpg",
                "조슈아 블로크",
                "36000",
                "32400",
                "인사이트",
                "20181101",
                "9788966262281",
                "자바 개발자를 위한 필독서"
            );

            // then
            assertThat(item.title()).isEqualTo("이펙티브 자바");
            assertThat(item.link()).isEqualTo("https://link.com");
            assertThat(item.image()).isEqualTo("https://image.com/book.jpg");
            assertThat(item.author()).isEqualTo("조슈아 블로크");
            assertThat(item.price()).isEqualTo("36000");
            assertThat(item.discount()).isEqualTo("32400");
            assertThat(item.publisher()).isEqualTo("인사이트");
            assertThat(item.pubdate()).isEqualTo("20181101");
            assertThat(item.isbn()).isEqualTo("9788966262281");
            assertThat(item.description()).isEqualTo("자바 개발자를 위한 필독서");
        }

        @Test
        @DisplayName("Given_NullFields_When_CreateRecord_Then_NullsArePreserved")
        void givenNullFields_whenCreateRecord_thenNullsArePreserved() {
            // given & when
            NaverBookItem item = new NaverBookItem(
                "테스트 책",
                null,
                null,
                "저자",
                null,
                null,
                null,
                null,
                "9781234567890",
                "설명"
            );

            // then
            assertThat(item.title()).isEqualTo("테스트 책");
            assertThat(item.link()).isNull();
            assertThat(item.image()).isNull();
            assertThat(item.price()).isNull();
            assertThat(item.pubdate()).isNull();
        }

        @Test
        @DisplayName("Given_AllNullFields_When_CreateRecord_Then_NoException")
        void givenAllNullFields_whenCreateRecord_thenNoException() {
            // when & then
            assertThatCode(() -> new NaverBookItem(
                null, null, null, null, null, null, null, null, null, null
            )).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("JSON Deserialization Tests")
    class JsonDeserializationTests {

        @Test
        @DisplayName("Given_CompleteJsonObject_When_Deserialize_Then_AllFieldsPopulated")
        void givenCompleteJsonObject_whenDeserialize_thenAllFieldsPopulated() throws Exception {
            // given
            String json = """
                {
                    "title": "이펙티브 자바",
                    "link": "https://link.com",
                    "image": "https://image.com/book.jpg",
                    "author": "조슈아 블로크",
                    "price": "36000",
                    "discount": "32400",
                    "publisher": "인사이트",
                    "pubdate": "20181101",
                    "isbn": "9788966262281",
                    "description": "자바 개발자를 위한 필독서"
                }
                """;

            // when
            NaverBookItem item = objectMapper.readValue(json, NaverBookItem.class);

            // then
            assertThat(item.title()).isEqualTo("이펙티브 자바");
            assertThat(item.isbn()).isEqualTo("9788966262281");
            assertThat(item.author()).isEqualTo("조슈아 블로크");
        }

        @Test
        @DisplayName("Given_PartialJsonObject_When_Deserialize_Then_MissingFieldsAreNull")
        void givenPartialJsonObject_whenDeserialize_thenMissingFieldsAreNull() throws Exception {
            // given
            String json = """
                {
                    "title": "테스트 책",
                    "isbn": "9781234567890"
                }
                """;

            // when
            NaverBookItem item = objectMapper.readValue(json, NaverBookItem.class);

            // then
            assertThat(item.title()).isEqualTo("테스트 책");
            assertThat(item.isbn()).isEqualTo("9781234567890");
            assertThat(item.author()).isNull();
            assertThat(item.publisher()).isNull();
            assertThat(item.description()).isNull();
        }

        @Test
        @DisplayName("Given_JsonWithUnknownFields_When_Deserialize_Then_IgnoresUnknownFields")
        void givenJsonWithUnknownFields_whenDeserialize_thenIgnoresUnknownFields() throws Exception {
            // given - @JsonIgnoreProperties(ignoreUnknown = true)
            String json = """
                {
                    "title": "테스트 책",
                    "isbn": "9781234567890",
                    "author": "저자",
                    "description": "설명",
                    "unknownField": "should be ignored",
                    "anotherUnknown": 12345
                }
                """;

            // when
            NaverBookItem item = objectMapper.readValue(json, NaverBookItem.class);

            // then - no exception thrown, unknown fields are ignored
            assertThat(item.title()).isEqualTo("테스트 책");
            assertThat(item.isbn()).isEqualTo("9781234567890");
        }

        @Test
        @DisplayName("Given_EmptyJsonObject_When_Deserialize_Then_AllFieldsNull")
        void givenEmptyJsonObject_whenDeserialize_thenAllFieldsNull() throws Exception {
            // given
            String json = "{}";

            // when
            NaverBookItem item = objectMapper.readValue(json, NaverBookItem.class);

            // then
            assertThat(item.title()).isNull();
            assertThat(item.isbn()).isNull();
            assertThat(item.author()).isNull();
        }

        @Test
        @DisplayName("Given_JsonWithNullValues_When_Deserialize_Then_NullValuesPreserved")
        void givenJsonWithNullValues_whenDeserialize_thenNullValuesPreserved() throws Exception {
            // given
            String json = """
                {
                    "title": "테스트 책",
                    "isbn": "9781234567890",
                    "author": null,
                    "publisher": null,
                    "description": "설명"
                }
                """;

            // when
            NaverBookItem item = objectMapper.readValue(json, NaverBookItem.class);

            // then
            assertThat(item.title()).isEqualTo("테스트 책");
            assertThat(item.author()).isNull();
            assertThat(item.publisher()).isNull();
        }
    }

    @Nested
    @DisplayName("JSON Serialization Tests")
    class JsonSerializationTests {

        @Test
        @DisplayName("Given_ValidRecord_When_Serialize_Then_ProducesValidJson")
        void givenValidRecord_whenSerialize_thenProducesValidJson() throws Exception {
            // given
            NaverBookItem item = new NaverBookItem(
                "이펙티브 자바",
                "https://link.com",
                "https://image.com/book.jpg",
                "조슈아 블로크",
                "36000",
                "32400",
                "인사이트",
                "20181101",
                "9788966262281",
                "자바 개발자를 위한 필독서"
            );

            // when
            String json = objectMapper.writeValueAsString(item);

            // then
            assertThat(json).contains("\"title\":\"이펙티브 자바\"");
            assertThat(json).contains("\"isbn\":\"9788966262281\"");
            assertThat(json).contains("\"author\":\"조슈아 블로크\"");
        }

        @Test
        @DisplayName("Given_RecordWithNulls_When_Serialize_Then_NullFieldsIncluded")
        void givenRecordWithNulls_whenSerialize_thenNullFieldsIncluded() throws Exception {
            // given
            NaverBookItem item = new NaverBookItem(
                "테스트",
                null,
                null,
                "저자",
                null,
                null,
                null,
                null,
                "9781234567890",
                "설명"
            );

            // when
            String json = objectMapper.writeValueAsString(item);

            // then
            assertThat(json).contains("\"title\":\"테스트\"");
            assertThat(json).contains("\"link\":null");
        }
    }

    @Nested
    @DisplayName("Record Equality Tests")
    class EqualityTests {

        @Test
        @DisplayName("Given_TwoIdenticalRecords_When_Compare_Then_AreEqual")
        void givenTwoIdenticalRecords_whenCompare_thenAreEqual() {
            // given
            NaverBookItem item1 = new NaverBookItem(
                "책", "link", "image", "author", "price", "discount",
                "publisher", "pubdate", "isbn", "desc"
            );
            NaverBookItem item2 = new NaverBookItem(
                "책", "link", "image", "author", "price", "discount",
                "publisher", "pubdate", "isbn", "desc"
            );

            // then
            assertThat(item1).isEqualTo(item2);
            assertThat(item1.hashCode()).isEqualTo(item2.hashCode());
        }

        @Test
        @DisplayName("Given_TwoDifferentRecords_When_Compare_Then_AreNotEqual")
        void givenTwoDifferentRecords_whenCompare_thenAreNotEqual() {
            // given
            NaverBookItem item1 = new NaverBookItem(
                "책1", "link", "image", "author", "price", "discount",
                "publisher", "pubdate", "isbn1", "desc"
            );
            NaverBookItem item2 = new NaverBookItem(
                "책2", "link", "image", "author", "price", "discount",
                "publisher", "pubdate", "isbn2", "desc"
            );

            // then
            assertThat(item1).isNotEqualTo(item2);
        }
    }

    @Nested
    @DisplayName("Special Character Handling Tests")
    class SpecialCharacterTests {

        @Test
        @DisplayName("Given_TitleWithHtmlTags_When_Deserialize_Then_PreservesHtmlTags")
        void givenTitleWithHtmlTags_whenDeserialize_thenPreservesHtmlTags() throws Exception {
            // given
            String json = """
                {
                    "title": "<b>볼드</b> 제목",
                    "isbn": "9781234567890",
                    "author": "저자",
                    "description": "설명"
                }
                """;

            // when
            NaverBookItem item = objectMapper.readValue(json, NaverBookItem.class);

            // then
            assertThat(item.title()).isEqualTo("<b>볼드</b> 제목");
        }

        @Test
        @DisplayName("Given_KoreanContent_When_SerializeAndDeserialize_Then_PreservesKorean")
        void givenKoreanContent_whenSerializeAndDeserialize_thenPreservesKorean() throws Exception {
            // given
            NaverBookItem original = new NaverBookItem(
                "객체지향의 사실과 오해",
                "https://link.com",
                "https://image.com/book.jpg",
                "조영호",
                "20000",
                "18000",
                "위키북스",
                "20150617",
                "9788998139766",
                "역할, 책임, 협력 관점에서 본 객체지향"
            );

            // when
            String json = objectMapper.writeValueAsString(original);
            NaverBookItem deserialized = objectMapper.readValue(json, NaverBookItem.class);

            // then
            assertThat(deserialized).isEqualTo(original);
        }
    }
}
