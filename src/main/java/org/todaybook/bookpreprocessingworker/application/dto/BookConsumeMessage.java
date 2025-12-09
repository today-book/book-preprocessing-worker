package org.todaybook.bookpreprocessingworker.application.dto;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public record BookConsumeMessage(
    String isbn,
    String title,
    List<String> categories,
    String description,
    String author,
    String publisher,
    LocalDate publishedAt,
    String thumbnail
) {}