package org.todaybook.bookpreprocessingworker.application.service;

import io.micrometer.common.util.StringUtils;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;
import org.todaybook.bookpreprocessingworker.application.port.in.BookMessageUseCase;
import org.todaybook.bookpreprocessingworker.application.port.out.BookMessagePublisher;
import org.todaybook.bookpreprocessingworker.domain.model.Book;

@Service
public class BookPreprocessingService implements BookMessageUseCase {

    private static final Logger log = LoggerFactory.getLogger(BookPreprocessingService.class);
    private static final int MIN_DESCRIPTION_LENGTH = 30;

    // ===================== Date Formats =====================

    private static final DateTimeFormatter NAVER_PUBDATE_FORMAT =
        DateTimeFormatter.ofPattern("yyyyMMdd");

    private static final DateTimeFormatter RAW_PUBDATE_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd");

    // ===================== Raw Row Column Index =====================

    private static final int RAW_ISBN_13_INDEX = 1;
    private static final int RAW_TITLE_INDEX = 3;
    private static final int RAW_AUTHOR_INDEX = 4;
    private static final int RAW_PUBLISHER_INDEX = 5;
    private static final int RAW_IMAGE_INDEX = 9;
    private static final int RAW_DESCRIPTION_INDEX = 10;
    private static final int RAW_PUBDATE_INDEX = 14;
    private static final int RAW_FALLBACK_ISBN_INDEX = 17;

    private static final Pattern HTML_TAG_PATTERN = Pattern.compile("<[^>]*>");
    private static final Pattern AUTHOR_SEPARATOR_PATTERN = Pattern.compile(
        "\\s*(\\^|;|\\||/|&|,|\\band\\b|\\+|·|ㆍ)\\s*",
        Pattern.CASE_INSENSITIVE
    );
    private static final Pattern AUTHOR_ROLE_PATTERN = Pattern.compile(
        "\\b(author|editor|translator|translated|illustrator|ed\\.|eds\\.|trans\\.)\\b",
        Pattern.CASE_INSENSITIVE
    );
    private static final Pattern AUTHOR_ROLE_KR_PATTERN = Pattern.compile(
        "(저자|지음|글쓴이|옮김|역자|번역|편저|편집|편역|감수|엮음|글씀|그림)"
    );

    private final BookMessagePublisher publisher;

    public BookPreprocessingService(BookMessagePublisher publisher) {
        this.publisher = publisher;
    }

    // =========================================================
    // RAW STRING PROCESSING
    // =========================================================

    @Override
    public void processRawRow(String rawRow) {
        if (StringUtils.isBlank(rawRow)) {
            log.warn("Skipping empty raw row");
            return;
        }

        List<String> columns = parseRawColumns(rawRow);
        if (columns.isEmpty()) {
            log.warn("Skipping malformed raw row. columnSize={}", columns.size());
            return;
        }

        String isbn = extractRawIsbn(columns);
        String title = cleanTitle(getColumn(columns, RAW_TITLE_INDEX));
        String author = normalizeAuthor(getColumn(columns, RAW_AUTHOR_INDEX));
        String description = normalizeDescription(
            firstNonBlank(
                getColumn(columns, RAW_DESCRIPTION_INDEX),
                getColumn(columns, RAW_DESCRIPTION_INDEX + 1)
            )
        );

        if (StringUtils.isBlank(isbn)) {
            log.warn("Skipping raw row: missing or invalid isbn");
            return;
        }
        if (StringUtils.isBlank(title)) {
            log.warn("Skipping raw row: missing title. isbn={}", isbn);
            return;
        }
        if (StringUtils.isBlank(author)) {
            log.warn("Skipping raw row: missing author. isbn={}", isbn);
            return;
        }
        if (StringUtils.isBlank(description)) {
            log.warn("Skipping raw row: missing/short description. isbn={}", isbn);
            return;
        }

        Book book = new Book(
            isbn,
            title,
            Collections.emptyList(),
            description,
            author,
            getColumn(columns, RAW_PUBLISHER_INDEX),
            parseRawPublishDate(getColumn(columns, RAW_PUBDATE_INDEX), isbn),
            normalizeThumbnail(getColumn(columns, RAW_IMAGE_INDEX))
        );

        log.info("Publishing book from RAW. isbn={}, title={}", book.isbn(), book.title());
        publisher.publish(book);
    }

    // =========================================================
    // NAVER API PROCESSING
    // =========================================================

    @Override
    public void processSingleItem(NaverBookItem item) {
        if (item == null) {
            log.warn("Skipping null Naver item");
            return;
        }

        String refinedIsbn = extractNormalizedIsbn(item.isbn());
        String title = cleanTitle(item.title());
        String author = normalizeAuthor(item.author());
        String description = normalizeDescription(item.description());

        if (StringUtils.isBlank(refinedIsbn)) {
            log.warn("Skipping Naver item: missing or invalid isbn");
            return;
        }
        if (StringUtils.isBlank(title)) {
            log.warn("Skipping Naver item: missing title. isbn={}", refinedIsbn);
            return;
        }
        if (StringUtils.isBlank(author)) {
            log.warn("Skipping Naver item: missing author. isbn={}", refinedIsbn);
            return;
        }
        if (StringUtils.isBlank(description)) {
            log.warn("Skipping Naver item: missing/short description. isbn={}", refinedIsbn);
            return;
        }

        Book book = new Book(
            refinedIsbn,
            title,
            Collections.emptyList(),
            description,
            author,
            item.publisher(),
            parsePublishDateToDate(item.pubdate(), refinedIsbn),
            normalizeThumbnail(item.image())
        );

        log.info("Publishing book from NAVER. isbn={}, title={}", book.isbn(), book.title());
        publisher.publish(book);
    }

    // =========================================================
    // Helpers
    // =========================================================

    private List<String> parseRawColumns(String row) {
        List<String> columns = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < row.length(); i++) {
            char c = row.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < row.length() && row.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                columns.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        columns.add(current.toString().trim());
        return columns;
    }

    private String extractRawIsbn(List<String> columns) {
        return firstNonBlank(
            normalizeIsbn(getColumn(columns, RAW_ISBN_13_INDEX)),
            normalizeIsbn(getColumn(columns, RAW_FALLBACK_ISBN_INDEX))
        );
    }

    private String normalizeIsbn(String raw) {
        if (StringUtils.isBlank(raw)) {
            return null;
        }

        String digits = raw.replaceAll("\\D", "");
        if (digits.length() == 13 || digits.length() == 10) {
            return digits;
        }

        return null;
    }

    private LocalDate parseRawPublishDate(String value, String isbn) {
        try {
            return StringUtils.isBlank(value)
                ? null
                : LocalDate.parse(value, RAW_PUBDATE_FORMAT);
        } catch (Exception e) {
            log.warn("Failed to parse RAW pubdate='{}' for isbn={}", value, isbn);
            return null;
        }
    }

    private LocalDate parsePublishDateToDate(String value, String isbn) {
        try {
            return StringUtils.isBlank(value)
                ? null
                : LocalDate.parse(value, NAVER_PUBDATE_FORMAT);
        } catch (Exception e) {
            log.warn("Failed to parse NAVER pubdate='{}' for isbn={}", value, isbn);
            return null;
        }
    }

    private String getColumn(List<String> columns, int index) {
        if (index < 0 || index >= columns.size()) return null;
        String value = columns.get(index);
        return StringUtils.isBlank(value) ? null : value.trim();
    }

    private String extractNormalizedIsbn(String raw) {
        if (StringUtils.isBlank(raw)) {
            return null;
        }

        String isbn10 = null;
        String[] tokens = raw.split("\\s+");
        for (String token : tokens) {
            String normalized = normalizeIsbn(token);
            if (normalized == null) {
                continue;
            }
            if (normalized.length() == 13) {
                return normalized;
            }
            if (normalized.length() == 10 && isbn10 == null) {
                isbn10 = normalized;
            }
        }
        return isbn10;
    }

    private String firstNonBlank(String... values) {
        for (String v : values) {
            if (StringUtils.isNotBlank(v)) {
                return v;
            }
        }
        return null;
    }

    private String cleanTitle(String title) {
        if (title == null) return null;
        return stripHtml(title).trim();
    }

    private String normalizeDescription(String raw) {
        if (StringUtils.isBlank(raw)) {
            return null;
        }

        String cleaned = stripHtml(raw)
            .replaceAll("\\s+", " ")
            .trim();

        if (cleaned.length() < MIN_DESCRIPTION_LENGTH) {
            return null;
        }

        return cleaned;
    }

    private String normalizeAuthor(String raw) {
        if (StringUtils.isBlank(raw)) {
            return null;
        }

        String cleaned = stripHtml(raw);
        String[] tokens = AUTHOR_SEPARATOR_PATTERN.split(cleaned, 2);
        String candidate = tokens.length > 0 ? tokens[0].trim() : cleaned.trim();

        if (StringUtils.isBlank(candidate)) {
            return null;
        }

        String withoutRoles = AUTHOR_ROLE_PATTERN.matcher(candidate).replaceAll("");
        withoutRoles = AUTHOR_ROLE_KR_PATTERN.matcher(withoutRoles).replaceAll("");
        withoutRoles = withoutRoles.replaceAll("\\s+", " ").trim();

        return StringUtils.isBlank(withoutRoles) ? null : withoutRoles;
    }

    private String normalizeThumbnail(String raw) {
        if (StringUtils.isBlank(raw)) {
            return null;
        }

        String trimmed = raw.trim();
        if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) {
            return trimmed;
        }
        return null;
    }

    private String stripHtml(String raw) {
        if (raw == null) {
            return "";
        }
        return HTML_TAG_PATTERN.matcher(raw).replaceAll("");
    }
}
