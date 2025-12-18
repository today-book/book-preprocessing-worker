package org.todaybook.bookpreprocessingworker.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.LocalDate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;
import org.todaybook.bookpreprocessingworker.application.port.out.BookMessagePublisher;
import org.todaybook.bookpreprocessingworker.domain.model.Book;

@ExtendWith(MockitoExtension.class)
@DisplayName("BookPreprocessingService - Domain/Port Tests")
class BookPreprocessingServiceTest {

    @Mock
    private BookMessagePublisher publisher;

    private BookPreprocessingService service;

    @BeforeEach
    void setUp() {
        service = new BookPreprocessingService(publisher);
    }

    @Nested
    class RawRowProcessing {
        @Test
        void processesValidRawRow() {
            String rawRow = "\"115982\",\"9780761921585\",\"cloth\",\"Designing for learning:six elements in constructivist classrooms\",\"George W. Gagnon, Jr., Michelle Collay\",\"Corwin Press, Calif.\",\"\",\"\",\"121081\",\"http://image.aladin.co.kr/product/519/70/cover/0761921583_1.jpg\",\"A great book about education and learning practices.\",\"\",\"designingforlearningsixelementsinconstructivistclassrooms\",\"\",\"2000-12-29\",\"Y\",\"Y\",\"0761921583 (cloth)\"";

            service.processRawRow(rawRow);

            ArgumentCaptor<Book> captor = ArgumentCaptor.forClass(Book.class);
            verify(publisher, times(1)).publish(captor.capture());

            Book book = captor.getValue();
            assertThat(book.isbn()).isEqualTo("9780761921585");
            assertThat(book.title()).isEqualTo("Designing for learning:six elements in constructivist classrooms");
            assertThat(book.author()).contains("George W. Gagnon");
            assertThat(book.publisher()).isEqualTo("Corwin Press, Calif.");
            assertThat(book.publishedAt()).isEqualTo(LocalDate.of(2000, 12, 29));
            assertThat(book.thumbnail()).contains("0761921583_1.jpg");
            assertThat(book.description()).isEqualTo("A great book about education and learning practices.");
        }

        @Test
        void usesFallbackIsbnWhenPrimaryMissing() {
            String rawRow = "\"id\",\"\",\"binding\",\"Title\",\"Author\",\"Publisher\",\"\",\"\",\"code\",\"http://image\",\"A sufficiently long description for validation.\",\"\",\"slug\",\"\",\"2000-12-29\",\"Y\",\"Y\",\"0761921583 (cloth)\"";

            service.processRawRow(rawRow);

            ArgumentCaptor<Book> captor = ArgumentCaptor.forClass(Book.class);
            verify(publisher).publish(captor.capture());
            assertThat(captor.getValue().isbn()).isEqualTo("0761921583");
        }

        @Test
        void skipsWhenRequiredFieldsMissing() {
            String rawRow = "\"id\",\"\",\"binding\",\"\",\"Author\",\"Publisher\",\"\",\"\",\"code\",\"http://image\",\"\",\"\",\"slug\",\"\",\"\",\"Y\",\"Y\",\"\"";

            service.processRawRow(rawRow);

            verify(publisher, never()).publish(org.mockito.Mockito.any());
        }
    }

    @Nested
    class JsonProcessing {

        @Test
        void processesValidJsonItem() {
            NaverBookItem item = new NaverBookItem(
                "<b>Title</b>",
                "http://link",
                "http://img",
                "Author",
                "10000",
                "9000",
                "Publisher",
                "20240102",
                "9781234567890",
                "A long enough description to pass validation rules."
            );

            service.processSingleItem(item);

            ArgumentCaptor<Book> captor = ArgumentCaptor.forClass(Book.class);
            verify(publisher).publish(captor.capture());

            Book book = captor.getValue();
            assertThat(book.isbn()).isEqualTo("9781234567890");
            assertThat(book.title()).isEqualTo("Title");
            assertThat(book.publishedAt()).isEqualTo(LocalDate.of(2024, 1, 2));
        }

        @Test
        void cleansHtmlInTitle() {
            NaverBookItem item = new NaverBookItem(
                "<b><i>Nested</i> Title</b>",
                null, null,
                "Author",
                null, null,
                "Publisher",
                "20240102",
                "9781234567890",
                "A long enough description to pass validation rules."
            );

            service.processSingleItem(item);

            ArgumentCaptor<Book> captor = ArgumentCaptor.forClass(Book.class);
            verify(publisher).publish(captor.capture());
            assertThat(captor.getValue().title()).isEqualTo("Nested Title");
        }

        @Test
        void extractsIsbn13WhenMixed() {
            NaverBookItem item = new NaverBookItem(
                "Title", null, null,
                "Author", null, null, "Publisher",
                "20240102",
                "1234567890 9789999999999",
                "A long enough description to pass validation rules."
            );

            service.processSingleItem(item);

            ArgumentCaptor<Book> captor = ArgumentCaptor.forClass(Book.class);
            verify(publisher).publish(captor.capture());
            assertThat(captor.getValue().isbn()).isEqualTo("9789999999999");
        }

        @Test
        void skipsInvalidItemWhenRequiredMissing() {
            NaverBookItem item = new NaverBookItem(
                "", null, null,
                "Author", null, null, "Publisher",
                "20240102",
                "123",
                ""
            );

            service.processSingleItem(item);

            verify(publisher, never()).publish(org.mockito.Mockito.any());
        }

        @Test
        void setsNullPublishedAtOnBadDate() {
            NaverBookItem item = new NaverBookItem(
                "Title", null, null,
                "Author", null, null, "Publisher",
                "bad-date",
                "9781234567890",
                "A long enough description to pass validation rules."
            );

            service.processSingleItem(item);

            ArgumentCaptor<Book> captor = ArgumentCaptor.forClass(Book.class);
            verify(publisher).publish(captor.capture());
            assertThat(captor.getValue().publishedAt()).isNull();
        }
    }
}
