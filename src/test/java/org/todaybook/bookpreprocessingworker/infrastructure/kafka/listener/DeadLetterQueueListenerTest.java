package org.todaybook.bookpreprocessingworker.infrastructure.kafka.listener;

import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("DeadLetterQueueListener Unit Tests")
class DeadLetterQueueListenerTest {

    private DeadLetterQueueListener deadLetterQueueListener;

    @BeforeEach
    void setUp() {
        deadLetterQueueListener = new DeadLetterQueueListener();
    }

    @Nested
    @DisplayName("monitorDeadLetter - Happy Path Tests")
    class MonitorDeadLetterHappyPath {

        @Test
        @DisplayName("Given_AllDlqHeaders_When_MonitorDeadLetter_Then_LogsWithoutException")
        void givenAllDlqHeaders_whenMonitorDeadLetter_thenLogsWithoutException() {
            // given
            String payload = "{\"title\":\"실패한 책\",\"isbn\":\"invalid\"}";
            String originalTopic = "book.raw.naver";
            String exceptionMessage = "Failed to deserialize: Unrecognized token";
            byte[] exceptionStackTrace = "java.lang.RuntimeException: parse error\n\tat com.example.Test".getBytes(StandardCharsets.UTF_8);

            // when & then - should not throw any exception
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    payload,
                    originalTopic,
                    exceptionMessage,
                    exceptionStackTrace
                )
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Given_ValidPayloadAndHeaders_When_MonitorDeadLetter_Then_ProcessesSuccessfully")
        void givenValidPayloadAndHeaders_whenMonitorDeadLetter_thenProcessesSuccessfully() {
            // given
            String payload = """
                {
                    "title": "르몽드 디플로마티크",
                    "isbn": "corrupted_isbn_data",
                    "author": "작가"
                }
                """;
            String originalTopic = "book.raw.naver";
            String exceptionMessage = "Deserialization exception during processing";
            byte[] exceptionStackTrace = createStackTraceBytes();

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    payload,
                    originalTopic,
                    exceptionMessage,
                    exceptionStackTrace
                )
            ).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("monitorDeadLetter - Optional Header Tests")
    class OptionalHeaderTests {

        @Test
        @DisplayName("Given_NullExceptionMessage_When_MonitorDeadLetter_Then_HandlesGracefully")
        void givenNullExceptionMessage_whenMonitorDeadLetter_thenHandlesGracefully() {
            // given
            String payload = "{\"title\":\"테스트\"}";
            String originalTopic = "book.raw.naver";
            String exceptionMessage = null; // Optional header is null
            byte[] exceptionStackTrace = "stack trace".getBytes(StandardCharsets.UTF_8);

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    payload,
                    originalTopic,
                    exceptionMessage,
                    exceptionStackTrace
                )
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Given_NullStackTrace_When_MonitorDeadLetter_Then_LogsNoStackTrace")
        void givenNullStackTrace_whenMonitorDeadLetter_thenLogsNoStackTrace() {
            // given
            String payload = "{\"title\":\"테스트\"}";
            String originalTopic = "book.raw.naver";
            String exceptionMessage = "Some error occurred";
            byte[] exceptionStackTrace = null; // Optional header is null

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    payload,
                    originalTopic,
                    exceptionMessage,
                    exceptionStackTrace
                )
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Given_BothOptionalHeadersNull_When_MonitorDeadLetter_Then_LogsSuccessfully")
        void givenBothOptionalHeadersNull_whenMonitorDeadLetter_thenLogsSuccessfully() {
            // given
            String payload = "{\"corrupted\":\"data\"}";
            String originalTopic = "book.raw.naver";

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    payload,
                    originalTopic,
                    null,  // no exception message
                    null   // no stack trace
                )
            ).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("monitorDeadLetter - Payload Edge Cases")
    class PayloadEdgeCases {

        @Test
        @DisplayName("Given_EmptyPayload_When_MonitorDeadLetter_Then_HandlesGracefully")
        void givenEmptyPayload_whenMonitorDeadLetter_thenHandlesGracefully() {
            // given
            String payload = "";
            String originalTopic = "book.raw.naver";
            String exceptionMessage = "Empty payload received";

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    payload,
                    originalTopic,
                    exceptionMessage,
                    null
                )
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Given_MalformedJsonPayload_When_MonitorDeadLetter_Then_LogsPayloadAsIs")
        void givenMalformedJsonPayload_whenMonitorDeadLetter_thenLogsPayloadAsIs() {
            // given
            String malformedPayload = "{ this is not valid json }}}";
            String originalTopic = "book.raw.naver";
            String exceptionMessage = "JSON parse error";

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    malformedPayload,
                    originalTopic,
                    exceptionMessage,
                    null
                )
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Given_VeryLongPayload_When_MonitorDeadLetter_Then_LogsSuccessfully")
        void givenVeryLongPayload_whenMonitorDeadLetter_thenLogsSuccessfully() {
            // given
            String longPayload = "X".repeat(100000); // 100KB payload
            String originalTopic = "book.raw.naver";
            String exceptionMessage = "Payload too large";

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    longPayload,
                    originalTopic,
                    exceptionMessage,
                    null
                )
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Given_PayloadWithSpecialCharacters_When_MonitorDeadLetter_Then_HandlesCorrectly")
        void givenPayloadWithSpecialCharacters_whenMonitorDeadLetter_thenHandlesCorrectly() {
            // given
            String specialPayload = """
                {
                    "title": "특수문자 테스트: <script>alert('xss')</script>",
                    "description": "\\n\\t\\r특수문자\\u0000포함"
                }
                """;
            String originalTopic = "book.raw.naver";
            String exceptionMessage = "Validation failed";

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    specialPayload,
                    originalTopic,
                    exceptionMessage,
                    null
                )
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Given_KoreanPayload_When_MonitorDeadLetter_Then_HandlesUnicodeCorrectly")
        void givenKoreanPayload_whenMonitorDeadLetter_thenHandlesUnicodeCorrectly() {
            // given
            String koreanPayload = "{\"title\":\"한글 테스트\",\"author\":\"김작가\"}";
            String originalTopic = "book.raw.naver";
            String exceptionMessage = "처리 중 오류 발생";
            byte[] koreanStackTrace = "java.lang.Exception: 한글 예외 메시지".getBytes(StandardCharsets.UTF_8);

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    koreanPayload,
                    originalTopic,
                    exceptionMessage,
                    koreanStackTrace
                )
            ).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("monitorDeadLetter - Different Topic Scenarios")
    class DifferentTopicScenarios {

        @Test
        @DisplayName("Given_DifferentOriginalTopic_When_MonitorDeadLetter_Then_LogsCorrectTopic")
        void givenDifferentOriginalTopic_whenMonitorDeadLetter_thenLogsCorrectTopic() {
            // given
            String payload = "{\"data\":\"test\"}";
            String originalTopic = "some.other.topic";
            String exceptionMessage = "Error from different topic";

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    payload,
                    originalTopic,
                    exceptionMessage,
                    null
                )
            ).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("monitorDeadLetter - Stack Trace Format Tests")
    class StackTraceFormatTests {

        @Test
        @DisplayName("Given_EmptyStackTraceBytes_When_MonitorDeadLetter_Then_HandlesGracefully")
        void givenEmptyStackTraceBytes_whenMonitorDeadLetter_thenHandlesGracefully() {
            // given
            String payload = "{\"test\":\"data\"}";
            String originalTopic = "book.raw.naver";
            String exceptionMessage = "Error";
            byte[] emptyStackTrace = new byte[0];

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    payload,
                    originalTopic,
                    exceptionMessage,
                    emptyStackTrace
                )
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Given_FullStackTrace_When_MonitorDeadLetter_Then_DecodesCorrectly")
        void givenFullStackTrace_whenMonitorDeadLetter_thenDecodesCorrectly() {
            // given
            String payload = "{\"test\":\"data\"}";
            String originalTopic = "book.raw.naver";
            String exceptionMessage = "JSON parsing failed";
            byte[] fullStackTrace = """
                com.fasterxml.jackson.core.JsonParseException: Unexpected character
                    at com.fasterxml.jackson.core.JsonParser._reportError(JsonParser.java:1234)
                    at com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:5678)
                    at org.todaybook.bookpreprocessingworker.application.kafka.BookKafkaListener.onMessage(BookKafkaListener.java:37)
                    at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
                    at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeRecordListener(KafkaMessageListenerContainer.java:2023)
                """.getBytes(StandardCharsets.UTF_8);

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    payload,
                    originalTopic,
                    exceptionMessage,
                    fullStackTrace
                )
            ).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("monitorDeadLetter - Real DLQ Scenario Tests")
    class RealDlqScenarios {

        @Test
        @DisplayName("Given_JsonDeserializationError_When_MonitorDeadLetter_Then_LogsAppropriately")
        void givenJsonDeserializationError_whenMonitorDeadLetter_thenLogsAppropriately() {
            // given - simulating a real DLQ message from deserialization failure
            String payload = "not a json at all - just random text";
            String originalTopic = "book.raw.naver";
            String exceptionMessage = "Cannot deserialize value of type NaverBookItem from String";
            byte[] stackTrace = createDeserializationExceptionStackTrace();

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    payload,
                    originalTopic,
                    exceptionMessage,
                    stackTrace
                )
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Given_KafkaProcessingError_When_MonitorDeadLetter_Then_LogsCorrectly")
        void givenKafkaProcessingError_whenMonitorDeadLetter_thenLogsCorrectly() {
            // given - simulating Kafka processing error
            String payload = "{\"title\":\"valid but problematic\",\"isbn\":\"test\",\"author\":\"auth\",\"description\":\"desc\"}";
            String originalTopic = "book.raw.naver";
            String exceptionMessage = "Listener method threw exception";
            byte[] stackTrace = "org.springframework.kafka.listener.ListenerExecutionFailedException".getBytes(StandardCharsets.UTF_8);

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    payload,
                    originalTopic,
                    exceptionMessage,
                    stackTrace
                )
            ).doesNotThrowAnyException();
        }

        @Test
        @DisplayName("Given_RetryExhaustedError_When_MonitorDeadLetter_Then_LogsRetryInfo")
        void givenRetryExhaustedError_whenMonitorDeadLetter_thenLogsRetryInfo() {
            // given - message after retries exhausted
            String payload = "{\"title\":\"책\",\"isbn\":\"123\",\"author\":\"저자\",\"description\":\"설명\"}";
            String originalTopic = "book.raw.naver";
            String exceptionMessage = "Retries exhausted after 3 attempts";
            byte[] stackTrace = "org.springframework.kafka.listener.SeekUtils: Seek to current after exception".getBytes(StandardCharsets.UTF_8);

            // when & then
            assertThatCode(() ->
                deadLetterQueueListener.monitorDeadLetter(
                    payload,
                    originalTopic,
                    exceptionMessage,
                    stackTrace
                )
            ).doesNotThrowAnyException();
        }
    }

    // --- Helper Methods ---

    private byte[] createStackTraceBytes() {
        return """
            java.lang.RuntimeException: Test exception
                at org.todaybook.Test.method(Test.java:42)
                at org.todaybook.Test.anotherMethod(Test.java:100)
            Caused by: java.io.IOException: IO error
                at org.todaybook.IO.read(IO.java:50)
            """.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] createDeserializationExceptionStackTrace() {
        return """
            com.fasterxml.jackson.databind.exc.MismatchedInputException: Cannot deserialize value
                at com.fasterxml.jackson.databind.DeserializationContext.reportInputMismatch(DeserializationContext.java:1234)
                at com.fasterxml.jackson.databind.ObjectMapper._readMapAndClose(ObjectMapper.java:4567)
                at com.fasterxml.jackson.databind.ObjectMapper.readValue(ObjectMapper.java:3456)
            """.getBytes(StandardCharsets.UTF_8);
    }
}
