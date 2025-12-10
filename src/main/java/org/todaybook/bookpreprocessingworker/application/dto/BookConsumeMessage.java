package org.todaybook.bookpreprocessingworker.application.dto;

import java.time.LocalDate;
import java.util.List;

public record BookConsumeMessage(
    String isbn,
    String title,
    List<String> categories,
    String description,
    // TODO: author 특수문자 처리 필요
    String author,
    String publisher,
    LocalDate publishedAt,
    // TODO: 줄바꿈 처리 필요
    String thumbnail
) {}