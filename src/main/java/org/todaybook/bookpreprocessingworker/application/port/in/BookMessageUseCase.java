package org.todaybook.bookpreprocessingworker.application.port.in;

import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;

public interface BookMessageUseCase {

    void processRawRow(String rawRow);

    void processSingleItem(NaverBookItem item);
}
