package org.todaybook.bookpreprocessingworker.application.dto;

import java.time.OffsetDateTime;
import java.time.LocalDate;

public record BookParsedMessage(
    String isbn,
    String title,
    String thumbnail,
    String description,
    String publisher,
    LocalDate publishedAt,
    OffsetDateTime createdAt,
    OffsetDateTime updatedAt
) {}