package org.todaybook.bookpreprocessingworker.application.kafka;

import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.times;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.todaybook.bookpreprocessingworker.application.service.BookPreprocessingService;

@ExtendWith(MockitoExtension.class)
@DisplayName("CsvBookKafkaListener Unit Tests")
class CsvBookKafkaListenerTest {

    @Mock
    private BookPreprocessingService preprocessingService;

    private CsvBookKafkaListener listener;

    @BeforeEach
    void setUp() {
        listener = new CsvBookKafkaListener(preprocessingService);
    }

    @Test
    @DisplayName("Given_CsvPayload_When_OnMessage_Then_DelegatesToCsvProcessor")
    void givenCsvPayload_whenOnMessage_thenDelegatesToCsvProcessor() {
        // given
        String csvPayload = "\"115982\",\"9780761921585\",\"cloth\",\"Title\",\"Author\",\"Publisher\",\"\",\"\",\"121081\",\"http://image\",\"\",\"\",\"slug\",\"\",\"2000-12-29\",\"Y\",\"Y\",\"0761921583 (cloth)\"";

        // when
        listener.onMessage(csvPayload);

        // then
        then(preprocessingService).should(times(1)).processCsvRow(csvPayload);
    }
}
