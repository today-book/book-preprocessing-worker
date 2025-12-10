package org.todaybook.bookpreprocessingworker.application.dto;

public record BookRawMessage(
    String isbn,
    String title,
    String description,
    String author,
    String thumbnail,   // Naver image
    String publisher,
    String pubdate      // "yyyyMMdd" string
) {}