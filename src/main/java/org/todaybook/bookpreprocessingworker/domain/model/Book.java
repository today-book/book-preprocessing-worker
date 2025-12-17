package org.todaybook.bookpreprocessingworker.domain.model;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

public record Book(
    String isbn,
    String title,
    List<String> categories,
    String description,
    String author,
    String publisher,
    LocalDate publishedAt,
    String thumbnail
) {
    public Book {
        categories = categories == null ? List.of() : List.copyOf(categories);
    }

    public String cleanIsbnKey() {
        return isbn == null ? null : isbn.trim();
    }
}
