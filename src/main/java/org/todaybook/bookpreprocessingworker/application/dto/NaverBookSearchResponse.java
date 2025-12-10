package org.todaybook.bookpreprocessingworker.application.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true) // JSON에 정의되지 않은 필드는 무시
public record NaverBookSearchResponse(
    String lastBuildDate,   // 응답 생성 시각
    int total,              // 전체 검색 결과 수
    int start,              // 현재 페이지 시작 위치
    int display,            // 현재 페이지 표시 개수
    List<NaverBookItem> items  // 실제 도서 데이터 리스트
) {}