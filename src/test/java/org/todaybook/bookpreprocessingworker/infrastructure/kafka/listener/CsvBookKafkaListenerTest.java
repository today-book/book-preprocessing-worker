package org.todaybook.bookpreprocessingworker.infrastructure.kafka.listener;

import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.todaybook.bookpreprocessingworker.application.port.in.BookMessageUseCase;
import org.todaybook.bookpreprocessingworker.config.AppKafkaProperties;
import org.todaybook.bookpreprocessingworker.config.TopicNames;

@ExtendWith(MockitoExtension.class)
@DisplayName("CsvBookKafkaListener Unit Tests")
class CsvBookKafkaListenerTest {

    @Mock
    private BookMessageUseCase bookMessageUseCase;

    private CsvBookKafkaListener listener;

    @BeforeEach
    void setUp() {
        listener = new CsvBookKafkaListener(bookMessageUseCase, topicNames());
    }

    @Test
    @DisplayName("Given_RawPayload_When_OnMessage_Then_DelegatesToRawProcessor")
    void givenRawPayload_whenOnMessage_thenDelegatesToRawProcessor() {
        // given
        String rawPayload = "\"115982\",\"9780761921585\",\"cloth\",\"Title\",\"Author\",\"Publisher\",\"\",\"\",\"121081\",\"http://image\",\"\",\"\",\"slug\",\"\",\"2000-12-29\",\"Y\",\"Y\",\"0761921583 (cloth)\"";

        // when
        listener.onMessage(rawPayload);

        // then
        then(bookMessageUseCase).should(times(1)).processRawRow(rawPayload);
    }

    private TopicNames topicNames() {
        AppKafkaProperties props = new AppKafkaProperties();
        props.setCsvInputTopic("book.raw.csv");
        return new TopicNames(props);
    }
}
