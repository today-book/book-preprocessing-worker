package org.todaybook.bookpreprocessingworker.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.todaybook.bookpreprocessingworker.application.dto.BookConsumeMessage;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;

@ExtendWith(MockitoExtension.class)
@DisplayName("BookPreprocessingService Unit Tests")
class BookPreprocessingServiceTest {

    private static final String OUTPUT_TOPIC = "book.parsed";

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private ObjectMapper objectMapper;
    private BookPreprocessingService preprocessingService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        preprocessingService = new BookPreprocessingService(kafkaTemplate, objectMapper);
    }

    @Nested
    @DisplayName("processCsvRow - CSV Parsing Tests")
    class ProcessCsvRowTests {

        @Test
        @DisplayName("Given_ValidCsvRow_When_ProcessCsvRow_Then_BuildsAndSendsMessage")
        void givenValidCsvRow_whenProcessCsvRow_thenBuildsAndSendsMessage() throws Exception {
            String csvRow = "\"115982\",\"9780761921585\",\"cloth\",\"Designing for learning:six elements in constructivist classrooms\",\"George W. Gagnon, Jr., Michelle Collay\",\"Corwin Press, Calif.\",\"\",\"\",\"121081\",\"http://image.aladin.co.kr/product/519/70/cover/0761921583_1.jpg\",\"\",\"\",\"designingforlearningsixelementsinconstructivistclassrooms\",\"\",\"2000-12-29\",\"Y\",\"Y\",\"0761921583 (cloth)\"";

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            preprocessingService.processCsvRow(csvRow);

            ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), eq("9780761921585"), jsonCaptor.capture());

            BookConsumeMessage message = objectMapper.readValue(jsonCaptor.getValue(), BookConsumeMessage.class);
            assertThat(message.isbn()).isEqualTo("9780761921585");
            assertThat(message.title()).isEqualTo("Designing for learning:six elements in constructivist classrooms");
            assertThat(message.author()).contains("George W. Gagnon");
            assertThat(message.publisher()).isEqualTo("Corwin Press, Calif.");
            assertThat(message.publishedAt()).isEqualTo(LocalDate.of(2000, 12, 29));
            assertThat(message.thumbnail()).contains("0761921583_1.jpg");
            assertThat(message.categories()).isEmpty();
        }

        @Test
        @DisplayName("Given_MissingPrimaryIsbn_When_ProcessCsvRow_Then_UsesFallbackIsbn")
        void givenMissingPrimaryIsbn_whenProcessCsvRow_thenUsesFallbackIsbn() {
            String csvRow = buildCsvRow(
                "115982",
                "", // isbn13 missing
                "cloth",
                "Fallback ISBN title",
                "Author Name",
                "Publisher",
                "", "", "121081",
                "http://image.aladin.co.kr/product/519/70/cover/0761921583_1.jpg",
                "", "",
                "slug-value",
                "",
                "2000-12-29",
                "Y",
                "Y",
                "0761921583 (cloth)"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            preprocessingService.processCsvRow(csvRow);

            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), eq("0761921583"), anyString());
        }

        @Test
        @DisplayName("Given_InvalidCsvRow_When_ProcessCsvRow_Then_SkipsSending")
        void givenInvalidCsvRow_whenProcessCsvRow_thenSkipsSending() {
            String csvRow = buildCsvRow(
                "id",
                "", // missing isbn and fallback
                "binding",
                "", // missing title
                "author",
                "publisher",
                "", "", "", "", "", "", "", "", "", "", "", ""
            );

            preprocessingService.processCsvRow(csvRow);

            verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
        }
    }

    @Nested
    @DisplayName("processSingleItem - Happy Path Tests")
    class ProcessSingleItemHappyPath {

        @Test
        @DisplayName("Given_ValidNaverBookItem_When_ProcessSingleItem_Then_SendsToKafka")
        void givenValidNaverBookItem_whenProcessSingleItem_thenSendsToKafka() {
            // given
            NaverBookItem item = createValidItem(
                "이펙티브 자바",
                "9788966262281",
                "20181101",
                "조슈아 블로크",
                "인사이트"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then
            verify(kafkaTemplate, times(1)).send(eq(OUTPUT_TOPIC), eq("9788966262281"), anyString());
        }

        @Test
        @DisplayName("Given_ValidItem_When_ProcessSingleItem_Then_CorrectlyParsesFields")
        void givenValidItem_whenProcessSingleItem_thenCorrectlyParsesFields() throws Exception {
            // given
            NaverBookItem item = createValidItem(
                "객체지향의 사실과 오해",
                "9788998139766",
                "20150617",
                "조영호",
                "위키북스"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then
            ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), eq("9788998139766"), jsonCaptor.capture());

            String sentJson = jsonCaptor.getValue();
            assertThat(sentJson).contains("\"isbn\":\"9788998139766\"");
            assertThat(sentJson).contains("\"title\":\"객체지향의 사실과 오해\"");
            assertThat(sentJson).contains("\"author\":\"조영호\"");
            assertThat(sentJson).contains("\"publisher\":\"위키북스\"");
        }

        @Test
        @DisplayName("Given_ItemWithValidDate_When_ProcessSingleItem_Then_ParsesDateCorrectly")
        void givenItemWithValidDate_whenProcessSingleItem_thenParsesDateCorrectly() throws Exception {
            // given
            NaverBookItem item = createValidItem(
                "테스트 책",
                "9781234567890",
                "20231225",
                "테스트 저자",
                "테스트 출판사"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then
            ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), anyString(), jsonCaptor.capture());

            String sentJson = jsonCaptor.getValue();
            // LocalDate serializes as array [2023,12,25] or ISO format depending on config
            assertThat(sentJson).containsAnyOf("2023,12,25", "2023-12-25");
        }
    }

    @Nested
    @DisplayName("processSingleItem - ISBN Parsing Tests")
    class IsbnParsingTests {

        @Test
        @DisplayName("Given_MixedIsbn_When_ProcessSingleItem_Then_Extracts13DigitIsbn")
        void givenMixedIsbn_whenProcessSingleItem_thenExtracts13DigitIsbn() {
            // given - ISBN format: "10자리 13자리"
            NaverBookItem item = createValidItem(
                "이펙티브 자바",
                "8966262287 9788966262281",
                "20181101",
                "조슈아 블로크",
                "인사이트"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then - should extract 13-digit ISBN
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), eq("9788966262281"), anyString());
        }

        @Test
        @DisplayName("Given_Only10DigitIsbn_When_ProcessSingleItem_Then_UsesFirstToken")
        void givenOnly10DigitIsbn_whenProcessSingleItem_thenUsesFirstToken() {
            // given - only 10-digit ISBN (no 13-digit available)
            NaverBookItem item = createValidItem(
                "레거시 북",
                "8966262287 1234567890",
                "20181101",
                "저자",
                "출판사"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then - falls back to first token
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), eq("8966262287"), anyString());
        }

        @Test
        @DisplayName("Given_SingleIsbn_When_ProcessSingleItem_Then_UsesAsIs")
        void givenSingleIsbn_whenProcessSingleItem_thenUsesAsIs() {
            // given
            NaverBookItem item = createValidItem(
                "단일 ISBN 책",
                "9788966262281",
                "20181101",
                "저자",
                "출판사"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), eq("9788966262281"), anyString());
        }

        @Test
        @DisplayName("Given_IsbnWithWhitespace_When_ProcessSingleItem_Then_TrimsIsbn")
        void givenIsbnWithWhitespace_whenProcessSingleItem_thenTrimsIsbn() {
            // given
            NaverBookItem item = createValidItem(
                "공백 ISBN 책",
                "  9788966262281  ",
                "20181101",
                "저자",
                "출판사"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), eq("9788966262281"), anyString());
        }
    }

    @Nested
    @DisplayName("processSingleItem - HTML Cleaning Tests")
    class HtmlCleaningTests {

        @ParameterizedTest
        @DisplayName("Given_TitleWithHtmlTags_When_ProcessSingleItem_Then_RemovesTags")
        @CsvSource({
            "'<b>볼드 제목</b>', '볼드 제목'",
            "'<i>이탤릭</i> 제목', '이탤릭 제목'",
            "'<b>헤드 퍼스트</b> 디자인 패턴', '헤드 퍼스트 디자인 패턴'",
            "'일반 제목', '일반 제목'",
            "'<span class=\"highlight\">복잡한</span> 태그', '복잡한 태그'"
        })
        void givenTitleWithHtmlTags_whenProcessSingleItem_thenRemovesTags(String inputTitle, String expectedTitle) throws Exception {
            // given
            NaverBookItem item = createValidItem(
                inputTitle,
                "9781234567890",
                "20231225",
                "저자",
                "출판사"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then
            ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), anyString(), jsonCaptor.capture());

            String sentJson = jsonCaptor.getValue();
            assertThat(sentJson).contains("\"title\":\"" + expectedTitle + "\"");
        }

        @Test
        @DisplayName("Given_TitleWithNestedTags_When_ProcessSingleItem_Then_RemovesAllTags")
        void givenTitleWithNestedTags_whenProcessSingleItem_thenRemovesAllTags() throws Exception {
            // given
            NaverBookItem item = createValidItem(
                "<b><i>중첩 태그</i> 제목</b>",
                "9781234567890",
                "20231225",
                "저자",
                "출판사"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then
            ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), anyString(), jsonCaptor.capture());

            String sentJson = jsonCaptor.getValue();
            assertThat(sentJson).contains("\"title\":\"중첩 태그 제목\"");
        }
    }

    @Nested
    @DisplayName("processSingleItem - Date Parsing Tests")
    class DateParsingTests {

        @Test
        @DisplayName("Given_InvalidDateFormat_When_ProcessSingleItem_Then_SetsNullAndSendsMessage")
        void givenInvalidDateFormat_whenProcessSingleItem_thenSetsNullAndSendsMessage() throws Exception {
            // given
            NaverBookItem item = createValidItem(
                "날짜 오류 책",
                "9781234567890",
                "INVALID_DATE",
                "저자",
                "출판사"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then - should still send, with null publishedAt
            ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), anyString(), jsonCaptor.capture());

            String sentJson = jsonCaptor.getValue();
            assertThat(sentJson).contains("\"publishedAt\":null");
        }

        @Test
        @DisplayName("Given_EmptyDateString_When_ProcessSingleItem_Then_SetsNullPublishedAt")
        void givenEmptyDateString_whenProcessSingleItem_thenSetsNullPublishedAt() throws Exception {
            // given
            NaverBookItem item = createValidItem(
                "빈 날짜 책",
                "9781234567890",
                "",
                "저자",
                "출판사"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then
            ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), anyString(), jsonCaptor.capture());

            String sentJson = jsonCaptor.getValue();
            assertThat(sentJson).contains("\"publishedAt\":null");
        }

        @Test
        @DisplayName("Given_NullDateString_When_ProcessSingleItem_Then_SetsNullPublishedAt")
        void givenNullDateString_whenProcessSingleItem_thenSetsNullPublishedAt() throws Exception {
            // given
            NaverBookItem item = new NaverBookItem(
                "날짜 없는 책",
                "http://link.com",
                "http://image.com",
                "저자",
                "20000",
                "18000",
                "출판사",
                null, // pubdate is null
                "9781234567890",
                "책 설명입니다."
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then
            ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), anyString(), jsonCaptor.capture());

            String sentJson = jsonCaptor.getValue();
            assertThat(sentJson).contains("\"publishedAt\":null");
        }

        @ParameterizedTest
        @DisplayName("Given_PartiallyInvalidDate_When_ProcessSingleItem_Then_SetsNull")
        @ValueSource(strings = {"2023", "202312", "2023-12-25", "25122023", "invalid"})
        void givenPartiallyInvalidDate_whenProcessSingleItem_thenSetsNull(String invalidDate) throws Exception {
            // given
            NaverBookItem item = createValidItem(
                "잘못된 날짜 책",
                "9781234567890",
                invalidDate,
                "저자",
                "출판사"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then
            ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), anyString(), jsonCaptor.capture());

            String sentJson = jsonCaptor.getValue();
            assertThat(sentJson).contains("\"publishedAt\":null");
        }
    }

    @Nested
    @DisplayName("processSingleItem - Validation Tests (Skip Invalid Items)")
    class ValidationTests {

        @Test
        @DisplayName("Given_MissingIsbn_When_ProcessSingleItem_Then_SkipsAndDoesNotSend")
        void givenMissingIsbn_whenProcessSingleItem_thenSkipsAndDoesNotSend() {
            // given
            NaverBookItem item = createItemWithMissingField("", "제목", "저자", "설명");

            // when
            preprocessingService.processSingleItem(item);

            // then
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
        }

        @Test
        @DisplayName("Given_MissingTitle_When_ProcessSingleItem_Then_SkipsAndDoesNotSend")
        void givenMissingTitle_whenProcessSingleItem_thenSkipsAndDoesNotSend() {
            // given
            NaverBookItem item = createItemWithMissingField("9781234567890", "", "저자", "설명");

            // when
            preprocessingService.processSingleItem(item);

            // then
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
        }

        @Test
        @DisplayName("Given_MissingAuthor_When_ProcessSingleItem_Then_SkipsAndDoesNotSend")
        void givenMissingAuthor_whenProcessSingleItem_thenSkipsAndDoesNotSend() {
            // given
            NaverBookItem item = createItemWithMissingField("9781234567890", "제목", "", "설명");

            // when
            preprocessingService.processSingleItem(item);

            // then
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
        }

        @Test
        @DisplayName("Given_MissingDescription_When_ProcessSingleItem_Then_SkipsAndDoesNotSend")
        void givenMissingDescription_whenProcessSingleItem_thenSkipsAndDoesNotSend() {
            // given
            NaverBookItem item = createItemWithMissingField("9781234567890", "제목", "저자", "");

            // when
            preprocessingService.processSingleItem(item);

            // then
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
        }

        @Test
        @DisplayName("Given_NullIsbn_When_ProcessSingleItem_Then_SkipsAndDoesNotSend")
        void givenNullIsbn_whenProcessSingleItem_thenSkipsAndDoesNotSend() {
            // given
            NaverBookItem item = createItemWithMissingField(null, "제목", "저자", "설명");

            // when
            preprocessingService.processSingleItem(item);

            // then
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
        }

        @Test
        @DisplayName("Given_WhitespaceOnlyIsbn_When_ProcessSingleItem_Then_SkipsAndDoesNotSend")
        void givenWhitespaceOnlyIsbn_whenProcessSingleItem_thenSkipsAndDoesNotSend() {
            // given
            NaverBookItem item = createItemWithMissingField("   ", "제목", "저자", "설명");

            // when
            preprocessingService.processSingleItem(item);

            // then
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
        }

        @Test
        @DisplayName("Given_AllFieldsNull_When_ProcessSingleItem_Then_SkipsAndDoesNotSend")
        void givenAllFieldsNull_whenProcessSingleItem_thenSkipsAndDoesNotSend() {
            // given
            NaverBookItem item = new NaverBookItem(
                null, null, null, null, null, null, null, null, null, null
            );

            // when
            preprocessingService.processSingleItem(item);

            // then
            verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
        }
    }

    @Nested
    @DisplayName("processSingleItem - Kafka Send Error Handling")
    class KafkaSendErrorTests {

        @Test
        @DisplayName("Given_KafkaSendFails_When_ProcessSingleItem_Then_LogsErrorAndContinues")
        void givenKafkaSendFails_whenProcessSingleItem_thenLogsErrorAndContinues() {
            // given
            NaverBookItem item = createValidItem(
                "에러 테스트 책",
                "9781234567890",
                "20231225",
                "저자",
                "출판사"
            );

            CompletableFuture<SendResult<String, Object>> failedFuture = new CompletableFuture<>();
            failedFuture.completeExceptionally(new RuntimeException("Kafka send failed"));

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(failedFuture);

            // when - should not throw exception
            preprocessingService.processSingleItem(item);

            // then
            verify(kafkaTemplate, times(1)).send(eq(OUTPUT_TOPIC), anyString(), anyString());
        }
    }

    @Nested
    @DisplayName("processSingleItem - Message Content Tests")
    class MessageContentTests {

        @Test
        @DisplayName("Given_ValidItem_When_ProcessSingleItem_Then_CategoriesIsEmptyList")
        void givenValidItem_whenProcessSingleItem_thenCategoriesIsEmptyList() throws Exception {
            // given
            NaverBookItem item = createValidItem(
                "테스트 책",
                "9781234567890",
                "20231225",
                "저자",
                "출판사"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then
            ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), anyString(), jsonCaptor.capture());

            String sentJson = jsonCaptor.getValue();
            assertThat(sentJson).contains("\"categories\":[]");
        }

        @Test
        @DisplayName("Given_ValidItem_When_ProcessSingleItem_Then_ThumbnailEqualsImage")
        void givenValidItem_whenProcessSingleItem_thenThumbnailEqualsImage() throws Exception {
            // given
            String expectedImage = "http://image.naver.com/book.jpg";
            NaverBookItem item = new NaverBookItem(
                "테스트 책",
                "http://link.com",
                expectedImage,
                "저자",
                "20000",
                "18000",
                "출판사",
                "20231225",
                "9781234567890",
                "책 설명입니다."
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then
            ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), anyString(), jsonCaptor.capture());

            String sentJson = jsonCaptor.getValue();
            assertThat(sentJson).contains("\"thumbnail\":\"" + expectedImage + "\"");
        }

        @Test
        @DisplayName("Given_ItemWithMultipleAuthors_When_ProcessSingleItem_Then_PreservesAuthorFormat")
        void givenItemWithMultipleAuthors_whenProcessSingleItem_thenPreservesAuthorFormat() throws Exception {
            // given - Naver uses ^ as author separator
            NaverBookItem item = createValidItem(
                "공동저자 책",
                "9781234567890",
                "20231225",
                "저자1^저자2^저자3",
                "출판사"
            );

            given(kafkaTemplate.send(anyString(), anyString(), any()))
                .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

            // when
            preprocessingService.processSingleItem(item);

            // then - author string is preserved as-is
            ArgumentCaptor<String> jsonCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(OUTPUT_TOPIC), anyString(), jsonCaptor.capture());

            String sentJson = jsonCaptor.getValue();
            assertThat(sentJson).contains("\"author\":\"저자1^저자2^저자3\"");
        }
    }

    // --- Helper Methods ---

    private String buildCsvRow(String... values) {
        return Arrays.stream(values)
            .map(value -> value == null ? "\"\"" : "\"" + value + "\"")
            .collect(Collectors.joining(","));
    }

    private NaverBookItem createValidItem(String title, String isbn, String pubdate, String author, String publisher) {
        return new NaverBookItem(
            title,
            "http://link.com",
            "http://image.com/book.jpg",
            author,
            "20000",
            "18000",
            publisher,
            pubdate,
            isbn,
            "이 책은 프로그래밍에 대한 깊은 통찰을 제공합니다."
        );
    }

    private NaverBookItem createItemWithMissingField(String isbn, String title, String author, String description) {
        return new NaverBookItem(
            title,
            "http://link.com",
            "http://image.com/book.jpg",
            author,
            "20000",
            "18000",
            "테스트출판사",
            "20231225",
            isbn,
            description
        );
    }
}
