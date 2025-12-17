package org.todaybook.bookpreprocessingworker.application.port.out;

import org.todaybook.bookpreprocessingworker.domain.model.Book;

public interface BookMessagePublisher {

    void publish(Book book);
}
