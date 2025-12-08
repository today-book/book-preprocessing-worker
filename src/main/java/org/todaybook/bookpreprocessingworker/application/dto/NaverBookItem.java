package org.todaybook.bookpreprocessingworker.application.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record NaverBookItem(
    String title,       // 책 제목 (HTML 태그 포함 가능)
    String link,        // 네이버 도서 링크
    String image,       // 표지 이미지 URL
    String author,      // 저자명 (여러명은 ^로 구분됨)
    String price,       // 정가 (문자열로 전달됨)
    String discount,    // 판매가
    String publisher,   // 출판사
    String pubdate,     // 출판일 (yyyyMMdd)
    String isbn,        // ISBN (10 or 13자리, 네이버는 종종 둘을 합쳐 보내기도 함)
    String description  // 책 설명
) {}