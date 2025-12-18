package org.todaybook.bookpreprocessingworker.support;

import java.util.Collections;
import java.util.List;
import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;

/**
 * Test fixtures and sample data generators for unit and integration tests.
 * Provides reusable test data creation methods following the Builder pattern.
 */
public final class TestFixtures {

    private TestFixtures() {
        // Utility class - prevent instantiation
    }

    // ===========================================
    // NaverBookItem Fixtures
    // ===========================================

    /**
     * Creates a valid NaverBookItem with all fields populated.
     */
    public static NaverBookItem createValidNaverBookItem() {
        return new NaverBookItem(
            "이펙티브 자바",
            "https://search.shopping.naver.com/book/catalog/123",
            "https://shopping-phinf.pstatic.net/main_123/123.jpg",
            "조슈아 블로크",
            "36000",
            "32400",
            "인사이트",
            "20181101",
            "9788966262281",
            "자바 개발자를 위한 필독서입니다."
        );
    }

    /**
     * Creates a NaverBookItem with only required fields.
     */
    public static NaverBookItem createMinimalNaverBookItem() {
        return new NaverBookItem(
            "테스트 책",
            null,
            null,
            "테스트 저자",
            null,
            null,
            null,
            null,
            "9781234567890",
            "테스트 설명입니다."
        );
    }

    /**
     * Creates a NaverBookItem with HTML tags in title.
     */
    public static NaverBookItem createNaverBookItemWithHtmlTitle() {
        return new NaverBookItem(
            "<b>헤드 퍼스트</b> 디자인 패턴",
            "https://link.com",
            "https://image.com/book.jpg",
            "에릭 프리먼",
            "36000",
            "32400",
            "한빛미디어",
            "20050901",
            "9788979143400",
            "디자인 패턴 입문서"
        );
    }

    /**
     * Creates a NaverBookItem with mixed ISBN format (10-digit + 13-digit).
     */
    public static NaverBookItem createNaverBookItemWithMixedIsbn() {
        return new NaverBookItem(
            "이펙티브 자바",
            "https://link.com",
            "https://image.com/book.jpg",
            "조슈아 블로크",
            "36000",
            "32400",
            "인사이트",
            "20181101",
            "8966262287 9788966262281",
            "자바 개발자를 위한 필독서"
        );
    }

    /**
     * Creates a NaverBookItem with invalid date format.
     */
    public static NaverBookItem createNaverBookItemWithInvalidDate() {
        return new NaverBookItem(
            "날짜 오류 책",
            "https://link.com",
            "https://image.com/book.jpg",
            "저자",
            "20000",
            "18000",
            "출판사",
            "INVALID_DATE",
            "9781234567890",
            "날짜 형식이 잘못된 책"
        );
    }

    /**
     * Creates a NaverBookItem with missing ISBN.
     */
    public static NaverBookItem createNaverBookItemWithMissingIsbn() {
        return new NaverBookItem(
            "ISBN 없는 책",
            "https://link.com",
            "https://image.com/book.jpg",
            "저자",
            "20000",
            "18000",
            "출판사",
            "20231225",
            "",
            "ISBN이 없는 책"
        );
    }

    /**
     * Creates a NaverBookItem with missing title.
     */
    public static NaverBookItem createNaverBookItemWithMissingTitle() {
        return new NaverBookItem(
            "",
            "https://link.com",
            "https://image.com/book.jpg",
            "저자",
            "20000",
            "18000",
            "출판사",
            "20231225",
            "9781234567890",
            "제목 없는 책"
        );
    }

    /**
     * Creates a NaverBookItem with multiple authors (separated by ^).
     */
    public static NaverBookItem createNaverBookItemWithMultipleAuthors() {
        return new NaverBookItem(
            "공동 저작 책",
            "https://link.com",
            "https://image.com/book.jpg",
            "저자1^저자2^저자3",
            "25000",
            "22500",
            "출판사",
            "20230615",
            "9789876543210",
            "여러 저자가 함께 쓴 책"
        );
    }

    /**
     * Creates a NaverBookItem resembling real Naver API response.
     */
    public static NaverBookItem createRealNaverApiItem() {
        return new NaverBookItem(
            "르몽드 디플로마티크(Le Monde Diplomatique)(한국어판)(2025년 12월호) (207호)",
            "https://search.shopping.naver.com/book/catalog/57944061835",
            "https://shopping-phinf.pstatic.net/main_5794406/57944061835.20251130072856.jpg",
            "브누아 브레빌^르몽드디플로마티크 편집부",
            "19000",
            "17100",
            "르몽드디플로마티크",
            "20251128",
            "9791192618944",
            "프랑스《르몽드》의 자매지로 전세계 27개 언어, 84개 국제판으로 발행되는 월간지"
        );
    }

    /**
     * Creates a customized NaverBookItem using builder pattern.
     */
    public static NaverBookItemBuilder naverBookItemBuilder() {
        return new NaverBookItemBuilder();
    }

    // ===========================================
    // NaverBookItem Collections Fixtures
    // ===========================================

    public static List<NaverBookItem> createSingleItemList() {
        return List.of(createValidNaverBookItem());
    }

    public static List<NaverBookItem> createMultipleItemList() {
        return List.of(
            createValidNaverBookItem(),
            createNaverBookItemWithHtmlTitle(),
            createNaverBookItemWithMixedIsbn()
        );
    }

    public static List<NaverBookItem> createEmptyItemList() {
        return Collections.emptyList();
    }

    // ===========================================
    // JSON Payload Fixtures
    // ===========================================

    /**
     * Creates a valid JSON payload for a single NaverBookItem.
     */
    public static String createValidJsonPayload() {
        return """
            {
                "title": "이펙티브 자바",
                "link": "https://search.shopping.naver.com/book/catalog/123",
                "image": "https://shopping-phinf.pstatic.net/main_123/123.jpg",
                "author": "조슈아 블로크",
                "price": "36000",
                "discount": "32400",
                "publisher": "인사이트",
                "pubdate": "20181101",
                "isbn": "9788966262281",
                "description": "자바 개발자를 위한 필독서입니다."
            }
            """;
    }

    /**
     * Creates a JSON payload with HTML title.
     */
    public static String createJsonPayloadWithHtmlTitle() {
        return """
            {
                "title": "<b>헤드 퍼스트</b> 디자인 패턴",
                "isbn": "9788979143400",
                "author": "에릭 프리먼",
                "publisher": "한빛미디어",
                "pubdate": "20050901",
                "description": "디자인 패턴 입문서"
            }
            """;
    }

    /**
     * Creates a JSON payload with mixed ISBN.
     */
    public static String createJsonPayloadWithMixedIsbn() {
        return """
            {
                "title": "이펙티브 자바",
                "isbn": "8966262287 9788966262281",
                "author": "조슈아 블로크",
                "publisher": "인사이트",
                "pubdate": "20181101",
                "description": "자바 개발자를 위한 필독서"
            }
            """;
    }

    /**
     * Creates an invalid JSON string.
     */
    public static String createInvalidJsonPayload() {
        return "{ this is not valid json }";
    }

    /**
     * Creates a JSON payload with missing required fields.
     */
    public static String createJsonPayloadWithMissingFields() {
        return """
            {
                "isbn": "9781234567890"
            }
            """;
    }

    // ===========================================
    // Builder Classes
    // ===========================================

    public static class NaverBookItemBuilder {
        private String title = "테스트 책";
        private String link = "https://link.com";
        private String image = "https://image.com/book.jpg";
        private String author = "테스트 저자";
        private String price = "20000";
        private String discount = "18000";
        private String publisher = "테스트 출판사";
        private String pubdate = "20231225";
        private String isbn = "9781234567890";
        private String description = "테스트 설명입니다.";

        public NaverBookItemBuilder title(String title) {
            this.title = title;
            return this;
        }

        public NaverBookItemBuilder link(String link) {
            this.link = link;
            return this;
        }

        public NaverBookItemBuilder image(String image) {
            this.image = image;
            return this;
        }

        public NaverBookItemBuilder author(String author) {
            this.author = author;
            return this;
        }

        public NaverBookItemBuilder price(String price) {
            this.price = price;
            return this;
        }

        public NaverBookItemBuilder discount(String discount) {
            this.discount = discount;
            return this;
        }

        public NaverBookItemBuilder publisher(String publisher) {
            this.publisher = publisher;
            return this;
        }

        public NaverBookItemBuilder pubdate(String pubdate) {
            this.pubdate = pubdate;
            return this;
        }

        public NaverBookItemBuilder isbn(String isbn) {
            this.isbn = isbn;
            return this;
        }

        public NaverBookItemBuilder description(String description) {
            this.description = description;
            return this;
        }

        public NaverBookItem build() {
            return new NaverBookItem(
                title, link, image, author, price, discount, publisher, pubdate, isbn, description
            );
        }
    }
}
