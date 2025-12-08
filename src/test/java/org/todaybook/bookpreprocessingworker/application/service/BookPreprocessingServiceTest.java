package org.todaybook.bookpreprocessingworker.application.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.todaybook.bookpreprocessingworker.application.dto.BookConsumeMessage;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookSearchResponse;

@ExtendWith(MockitoExtension.class)
class BookPreprocessingServiceTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private BookPreprocessingService preprocessingService;

    @Test
    @DisplayName("Test 1: 정상적인 르몽드 도서 데이터 3건이 주어졌을 때, 모두 정상적으로 파싱되어 Kafka로 전송된다.")
    void process_StandardData() {
        // given
        NaverBookItem item1 = createItem(
            "르몽드 디플로마티크 12월호", "9791192618944", "20251128", "브누아 브레빌^편집부", "url1"
        );
        NaverBookItem item2 = createItem(
            "르몽드 디플로마티크 11월호", "9791192618906", "20251030", "브누아 브레빌", "url2"
        );
        NaverBookItem item3 = createItem(
            "객체지향의 사실과 오해", "9788998139766", "20150617", "조영호", "url3"
        );

        NaverBookSearchResponse response = new NaverBookSearchResponse(
            "Fri, 05 Dec 2025", 131, 1, 100, List.of(item1, item2, item3)
        );

        // KafkaTemplate send 모킹 (성공했다고 가정)
        given(kafkaTemplate.send(anyString(), anyString(), any()))
            .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        // when
        preprocessingService.process(response);

        // then
        // 1. send가 총 3번 호출되었는지 검증
        verify(kafkaTemplate, times(3)).send(eq("book.parsed"), anyString(), any());

        // 2. 전송된 메시지 내용 검증 (ArgumentCaptor 사용)
        ArgumentCaptor<BookConsumeMessage> captor = ArgumentCaptor.forClass(BookConsumeMessage.class);
        verify(kafkaTemplate, times(3)).send(eq("book.parsed"), anyString(), captor.capture());

        List<BookConsumeMessage> capturedValues = captor.getAllValues();

        // 첫 번째 아이템 검증
        BookConsumeMessage msg1 = capturedValues.get(0);
        assertThat(msg1.isbn()).isEqualTo("9791192618944");
        assertThat(msg1.title()).isEqualTo("르몽드 디플로마티크 12월호");
        assertThat(msg1.author()).isEqualTo("브누아 브레빌^편집부"); // 원본 유지 확인
        assertThat(msg1.publishedAt()).isEqualTo(LocalDateTime.of(2025, 11, 28, 0, 0, 0));

        // 세 번째 아이템 검증
        BookConsumeMessage msg3 = capturedValues.get(2);
        assertThat(msg3.isbn()).isEqualTo("9788998139766");
        assertThat(msg3.publisher()).isEqualTo("테스트출판사"); // 헬퍼 메서드 기본값
    }

    @Test
    @DisplayName("Test 2: 더러운 데이터(HTML 태그, ISBN 혼합)가 섞여 있어도 정제하여 전송한다.")
    void process_DirtyData() {
        // given
        // Case A: ISBN이 '10자리 13자리' 공백으로 섞여 있음 -> 13자리 추출
        NaverBookItem mixedIsbnItem = createItem(
            "이펙티브 자바", "8966262287 9788966262281", "20181101", "조슈아 블로크", "img"
        );

        // Case B: 제목에 <b> 태그가 포함됨 -> 태그 제거
        NaverBookItem htmlTitleItem = createItem(
            "<b>헤드 퍼스트</b> 디자인 패턴", "9788979143400", "20050901", "에릭 프리먼", "img"
        );

        // Case C: 날짜 형식이 이상함 -> 날짜 null로 처리하되 메시지는 전송
        NaverBookItem invalidDateItem = createItem(
            "날짜가 이상한 책", "9781111222233", "INVALID_DATE", "저자미상", "img"
        );

        NaverBookSearchResponse response = new NaverBookSearchResponse(
            "Fri, 05 Dec 2025", 3, 1, 3, List.of(mixedIsbnItem, htmlTitleItem, invalidDateItem)
        );

        given(kafkaTemplate.send(anyString(), anyString(), any()))
            .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        // when
        preprocessingService.process(response);

        // then
        ArgumentCaptor<BookConsumeMessage> captor = ArgumentCaptor.forClass(BookConsumeMessage.class);
        verify(kafkaTemplate, times(3)).send(eq("book.parsed"), anyString(), captor.capture());
        List<BookConsumeMessage> results = captor.getAllValues();

        // Case A 검증: ISBN 13자리 추출 확인
        assertThat(results.get(0).isbn()).isEqualTo("9788966262281");

        // Case B 검증: HTML 태그 제거 확인
        assertThat(results.get(1).title()).isEqualTo("헤드 퍼스트 디자인 패턴");

        // Case C 검증: 날짜 파싱 실패 시 null 처리 확인
        assertThat(results.get(2).publishedAt()).isNull();
    }

    @Test
    @DisplayName("Test 3: 필수값(ISBN, Title)이 누락된 아이템은 전송하지 않고 건너뛴다.")
    void process_SkipInvalidItems() {
        // given
        // Valid Item
        NaverBookItem validItem = createItem("정상 책", "9780000000001", "20250101", "저자", "img");

        // Invalid: ISBN 없음
        NaverBookItem noIsbnItem = createItem("ISBN 없는 책", "", "20250101", "저자", "img");

        // Invalid: Title 없음
        NaverBookItem noTitleItem = createItem("", "9780000000002", "20250101", "저자", "img");

        NaverBookSearchResponse response = new NaverBookSearchResponse(
            "Fri, 05 Dec 2025", 3, 1, 3, List.of(validItem, noIsbnItem, noTitleItem)
        );

        given(kafkaTemplate.send(anyString(), anyString(), any()))
            .willReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        // when
        preprocessingService.process(response);

        // then
        // 3개가 들어왔지만 유효한 건 1개뿐이므로 1번만 호출되어야 함
        verify(kafkaTemplate, times(1)).send(eq("book.parsed"), anyString(), any());
    }

    // --- Helper Method to create NaverBookItem ---
    private NaverBookItem createItem(String title, String isbn, String pubdate, String author, String image) {
        return new NaverBookItem(
            title,
            "http://link.com",
            image,
            author,
            "20000", // price
            "18000", // discount
            "테스트출판사", // publisher
            pubdate,
            isbn,
            "책 설명입니다."
        );
    }
}