package org.todaybook.bookpreprocessingworker.application.kafka;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;
import org.todaybook.bookpreprocessingworker.application.service.BookPreprocessingService;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
@DisplayName("BookKafkaListener Unit Tests")
class BookKafkaListenerTest {

    @Mock
    private BookPreprocessingService preprocessingService;

    private ObjectMapper objectMapper;
    private BookKafkaListener bookKafkaListener;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        bookKafkaListener = new BookKafkaListener(preprocessingService, objectMapper);
    }

    @Nested
    @DisplayName("onMessage - Happy Path Tests")
    class OnMessageHappyPath {

        @Test
        @DisplayName("Given_ValidJsonPayload_When_OnMessage_Then_DelegatesToService")
        void givenValidJsonPayload_whenOnMessage_thenDelegatesToService() throws JsonProcessingException {
            // given
            String jsonPayload = """
                {
                    "title": "이펙티브 자바",
                    "link": "http://link.com",
                    "image": "http://image.com",
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
            bookKafkaListener.onMessage(jsonPayload);

            // then
            ArgumentCaptor<NaverBookItem> captor = ArgumentCaptor.forClass(NaverBookItem.class);
            verify(preprocessingService, times(1)).processSingleItem(captor.capture());

            NaverBookItem capturedItem = captor.getValue();
            assertThat(capturedItem.title()).isEqualTo("이펙티브 자바");
            assertThat(capturedItem.isbn()).isEqualTo("9788966262281");
            assertThat(capturedItem.author()).isEqualTo("조슈아 블로크");
            assertThat(capturedItem.publisher()).isEqualTo("인사이트");
        }

        @Test
        @DisplayName("Given_MinimalValidPayload_When_OnMessage_Then_ParsesSuccessfully")
        void givenMinimalValidPayload_whenOnMessage_thenParsesSuccessfully() throws JsonProcessingException {
            // given - only required fields
            String jsonPayload = """
                {
                    "title": "테스트 책",
                    "isbn": "9781234567890",
                    "author": "저자",
                    "description": "설명"
                }
                """;

            // when
            bookKafkaListener.onMessage(jsonPayload);

            // then
            ArgumentCaptor<NaverBookItem> captor = ArgumentCaptor.forClass(NaverBookItem.class);
            verify(preprocessingService).processSingleItem(captor.capture());

            NaverBookItem capturedItem = captor.getValue();
            assertThat(capturedItem.title()).isEqualTo("테스트 책");
            assertThat(capturedItem.isbn()).isEqualTo("9781234567890");
            // Optional fields should be null
            assertThat(capturedItem.link()).isNull();
            assertThat(capturedItem.image()).isNull();
        }

        @Test
        @DisplayName("Given_PayloadWithHtmlTitle_When_OnMessage_Then_PreservesRawTitle")
        void givenPayloadWithHtmlTitle_whenOnMessage_thenPreservesRawTitle() throws JsonProcessingException {
            // given - title with HTML tags (cleaning happens in service)
            String jsonPayload = """
                {
                    "title": "<b>헤드 퍼스트</b> 디자인 패턴",
                    "isbn": "9788979143400",
                    "author": "에릭 프리먼",
                    "description": "디자인 패턴 입문서"
                }
                """;

            // when
            bookKafkaListener.onMessage(jsonPayload);

            // then - raw title is passed to service (service handles cleaning)
            ArgumentCaptor<NaverBookItem> captor = ArgumentCaptor.forClass(NaverBookItem.class);
            verify(preprocessingService).processSingleItem(captor.capture());

            NaverBookItem capturedItem = captor.getValue();
            assertThat(capturedItem.title()).isEqualTo("<b>헤드 퍼스트</b> 디자인 패턴");
        }

        @Test
        @DisplayName("Given_PayloadWithKoreanContent_When_OnMessage_Then_HandlesUnicodeCorrectly")
        void givenPayloadWithKoreanContent_whenOnMessage_thenHandlesUnicodeCorrectly() throws JsonProcessingException {
            // given
            String jsonPayload = """
                {
                    "title": "객체지향의 사실과 오해",
                    "isbn": "9788998139766",
                    "author": "조영호",
                    "publisher": "위키북스",
                    "description": "역할, 책임, 협력 관점에서 본 객체지향"
                }
                """;

            // when
            bookKafkaListener.onMessage(jsonPayload);

            // then
            ArgumentCaptor<NaverBookItem> captor = ArgumentCaptor.forClass(NaverBookItem.class);
            verify(preprocessingService).processSingleItem(captor.capture());

            NaverBookItem capturedItem = captor.getValue();
            assertThat(capturedItem.title()).isEqualTo("객체지향의 사실과 오해");
            assertThat(capturedItem.description()).isEqualTo("역할, 책임, 협력 관점에서 본 객체지향");
        }
    }

    @Nested
    @DisplayName("onMessage - Error Handling Tests")
    class OnMessageErrorHandling {

        @Test
        @DisplayName("Given_InvalidJson_When_OnMessage_Then_ThrowsJsonProcessingException")
        void givenInvalidJson_whenOnMessage_thenThrowsJsonProcessingException() {
            // given
            String invalidJson = "{ invalid json }";

            // when & then
            assertThatThrownBy(() -> bookKafkaListener.onMessage(invalidJson))
                .isInstanceOf(JsonProcessingException.class);

            // Service should not be called
            verify(preprocessingService, never()).processSingleItem(any());
        }

        @Test
        @DisplayName("Given_EmptyJson_When_OnMessage_Then_ThrowsJsonProcessingException")
        void givenEmptyJson_whenOnMessage_thenThrowsJsonProcessingException() {
            // given
            String emptyJson = "";

            // when & then
            assertThatThrownBy(() -> bookKafkaListener.onMessage(emptyJson))
                .isInstanceOf(JsonProcessingException.class);

            verify(preprocessingService, never()).processSingleItem(any());
        }

        @Test
        @DisplayName("Given_ArrayInsteadOfObject_When_OnMessage_Then_ThrowsJsonProcessingException")
        void givenArrayInsteadOfObject_whenOnMessage_thenThrowsJsonProcessingException() {
            // given - array format instead of single object
            String arrayJson = """
                [{
                    "title": "책1",
                    "isbn": "1234567890123",
                    "author": "저자1",
                    "description": "설명1"
                }]
                """;

            // when & then
            assertThatThrownBy(() -> bookKafkaListener.onMessage(arrayJson))
                .isInstanceOf(JsonProcessingException.class);

            verify(preprocessingService, never()).processSingleItem(any());
        }

        @Test
        @DisplayName("Given_NullPayload_When_OnMessage_Then_ThrowsException")
        void givenNullPayload_whenOnMessage_thenThrowsException() {
            // given
            String nullPayload = null;

            // when & then
            assertThatThrownBy(() -> bookKafkaListener.onMessage(nullPayload))
                .isInstanceOf(Exception.class);

            verify(preprocessingService, never()).processSingleItem(any());
        }
    }

    @Nested
    @DisplayName("onMessage - Edge Case Tests")
    class OnMessageEdgeCases {

        @Test
        @DisplayName("Given_PayloadWithExtraFields_When_OnMessage_Then_IgnoresUnknownFields")
        void givenPayloadWithExtraFields_whenOnMessage_thenIgnoresUnknownFields() throws JsonProcessingException {
            // given - JSON with unknown fields (should be ignored due to @JsonIgnoreProperties)
            String jsonPayload = """
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
            bookKafkaListener.onMessage(jsonPayload);

            // then - should parse successfully, ignoring unknown fields
            ArgumentCaptor<NaverBookItem> captor = ArgumentCaptor.forClass(NaverBookItem.class);
            verify(preprocessingService).processSingleItem(captor.capture());

            NaverBookItem capturedItem = captor.getValue();
            assertThat(capturedItem.title()).isEqualTo("테스트 책");
        }

        @Test
        @DisplayName("Given_PayloadWithNullValues_When_OnMessage_Then_PreservesNulls")
        void givenPayloadWithNullValues_whenOnMessage_thenPreservesNulls() throws JsonProcessingException {
            // given
            String jsonPayload = """
                {
                    "title": "테스트 책",
                    "isbn": "9781234567890",
                    "author": "저자",
                    "description": "설명",
                    "link": null,
                    "image": null,
                    "pubdate": null
                }
                """;

            // when
            bookKafkaListener.onMessage(jsonPayload);

            // then
            ArgumentCaptor<NaverBookItem> captor = ArgumentCaptor.forClass(NaverBookItem.class);
            verify(preprocessingService).processSingleItem(captor.capture());

            NaverBookItem capturedItem = captor.getValue();
            assertThat(capturedItem.link()).isNull();
            assertThat(capturedItem.image()).isNull();
            assertThat(capturedItem.pubdate()).isNull();
        }

        @Test
        @DisplayName("Given_PayloadWithMixedIsbn_When_OnMessage_Then_PreservesRawIsbn")
        void givenPayloadWithMixedIsbn_whenOnMessage_thenPreservesRawIsbn() throws JsonProcessingException {
            // given - ISBN parsing is done by service, not listener
            String jsonPayload = """
                {
                    "title": "이펙티브 자바",
                    "isbn": "8966262287 9788966262281",
                    "author": "조슈아 블로크",
                    "description": "자바 개발자 필독서"
                }
                """;

            // when
            bookKafkaListener.onMessage(jsonPayload);

            // then - raw ISBN is passed to service
            ArgumentCaptor<NaverBookItem> captor = ArgumentCaptor.forClass(NaverBookItem.class);
            verify(preprocessingService).processSingleItem(captor.capture());

            NaverBookItem capturedItem = captor.getValue();
            assertThat(capturedItem.isbn()).isEqualTo("8966262287 9788966262281");
        }

        @Test
        @DisplayName("Given_EmptyObjectPayload_When_OnMessage_Then_ParsesWithNullFields")
        void givenEmptyObjectPayload_whenOnMessage_thenParsesWithNullFields() throws JsonProcessingException {
            // given
            String emptyObjectJson = "{}";

            // when
            bookKafkaListener.onMessage(emptyObjectJson);

            // then - all fields are null, but still parsed
            ArgumentCaptor<NaverBookItem> captor = ArgumentCaptor.forClass(NaverBookItem.class);
            verify(preprocessingService).processSingleItem(captor.capture());

            NaverBookItem capturedItem = captor.getValue();
            assertThat(capturedItem.title()).isNull();
            assertThat(capturedItem.isbn()).isNull();
            assertThat(capturedItem.author()).isNull();
        }

        @Test
        @DisplayName("Given_PayloadWithSpecialCharacters_When_OnMessage_Then_HandlesCorrectly")
        void givenPayloadWithSpecialCharacters_whenOnMessage_thenHandlesCorrectly() throws JsonProcessingException {
            // given
            String jsonPayload = """
                {
                    "title": "C++ 프로그래밍: '입문'에서 \\"고급\\"까지",
                    "isbn": "9781234567890",
                    "author": "저자 (역자: 홍길동)",
                    "description": "특수문자 테스트: <, >, &, ', \\""
                }
                """;

            // when
            bookKafkaListener.onMessage(jsonPayload);

            // then
            ArgumentCaptor<NaverBookItem> captor = ArgumentCaptor.forClass(NaverBookItem.class);
            verify(preprocessingService).processSingleItem(captor.capture());

            NaverBookItem capturedItem = captor.getValue();
            assertThat(capturedItem.title()).contains("C++");
            assertThat(capturedItem.description()).contains("<");
        }

        @Test
        @DisplayName("Given_VeryLongPayload_When_OnMessage_Then_HandlesCorrectly")
        void givenVeryLongPayload_whenOnMessage_thenHandlesCorrectly() throws JsonProcessingException {
            // given - long description
            String longDescription = "A".repeat(10000);
            String jsonPayload = String.format("""
                {
                    "title": "긴 설명 책",
                    "isbn": "9781234567890",
                    "author": "저자",
                    "description": "%s"
                }
                """, longDescription);

            // when
            bookKafkaListener.onMessage(jsonPayload);

            // then
            ArgumentCaptor<NaverBookItem> captor = ArgumentCaptor.forClass(NaverBookItem.class);
            verify(preprocessingService).processSingleItem(captor.capture());

            NaverBookItem capturedItem = captor.getValue();
            assertThat(capturedItem.description()).hasSize(10000);
        }
    }

    @Nested
    @DisplayName("onMessage - Real-world Payload Tests")
    class RealWorldPayloadTests {

        @Test
        @DisplayName("Given_RealNaverApiResponse_When_OnMessage_Then_ParsesAllFieldsCorrectly")
        void givenRealNaverApiResponse_whenOnMessage_thenParsesAllFieldsCorrectly() throws JsonProcessingException {
            // given - realistic Naver API response
            String jsonPayload = """
                {
                    "title": "르몽드 디플로마티크(Le Monde Diplomatique)(한국어판)(2025년 12월호)",
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
            bookKafkaListener.onMessage(jsonPayload);

            // then
            ArgumentCaptor<NaverBookItem> captor = ArgumentCaptor.forClass(NaverBookItem.class);
            verify(preprocessingService).processSingleItem(captor.capture());

            NaverBookItem capturedItem = captor.getValue();
            assertThat(capturedItem.title()).contains("르몽드 디플로마티크");
            assertThat(capturedItem.isbn()).isEqualTo("9791192618944");
            assertThat(capturedItem.pubdate()).isEqualTo("20251128");
            assertThat(capturedItem.author()).contains("^"); // Multiple authors
            assertThat(capturedItem.discount()).isEqualTo("17100");
        }
    }
}
