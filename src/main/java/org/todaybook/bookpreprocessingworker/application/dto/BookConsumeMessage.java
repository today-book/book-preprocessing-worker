package org.todaybook.bookpreprocessingworker.application.dto;

import java.time.LocalDate;
import java.util.List;

/**
 * Outbound DTO published to Kafka after preprocessing.
 */
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
