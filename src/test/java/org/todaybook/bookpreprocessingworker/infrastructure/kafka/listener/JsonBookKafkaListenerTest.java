package org.todaybook.bookpreprocessingworker.infrastructure.kafka.listener;

import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;
import org.todaybook.bookpreprocessingworker.application.port.in.BookMessageUseCase;
import org.todaybook.bookpreprocessingworker.config.AppKafkaProperties;
import org.todaybook.bookpreprocessingworker.config.TopicNames;

@ExtendWith(MockitoExtension.class)
@DisplayName("JsonBookKafkaListener Unit Tests")
class JsonBookKafkaListenerTest {

    @Mock
    private BookMessageUseCase bookMessageUseCase;

    private JsonBookKafkaListener listener;

    @BeforeEach
    void setUp() {
        listener = new JsonBookKafkaListener(bookMessageUseCase, topicNames());
    }

    @Test
    @DisplayName("Given_ValidPayload_When_OnMessage_Then_DelegatesToUseCase")
    void givenValidPayload_whenOnMessage_thenDelegatesToUseCase() {
        NaverBookItem payload = new NaverBookItem(
            "Title",
            "http://link",
            "http://image",
            "Author",
            null,
            null,
            "Publisher",
            "20240102",
            "9781234567890",
            "A long enough description to pass validation rules."
        );

        listener.onMessage(payload);

        then(bookMessageUseCase).should(times(1)).processSingleItem(payload);
    }

    @Test
    @DisplayName("Given_NullPayload_When_OnMessage_Then_DoesNotDelegate")
    void givenNullPayload_whenOnMessage_thenDoesNotDelegate() {
        listener.onMessage(null);

        then(bookMessageUseCase).should(never()).processSingleItem(null);
    }

    private TopicNames topicNames() {
        AppKafkaProperties props = new AppKafkaProperties();
        props.setInputTopic("book.raw.naver");
        return new TopicNames(props);
    }
}
