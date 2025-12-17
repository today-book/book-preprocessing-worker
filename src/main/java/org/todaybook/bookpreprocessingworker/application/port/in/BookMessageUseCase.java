package org.todaybook.bookpreprocessingworker.application.port.in;

import org.todaybook.bookpreprocessingworker.application.dto.NaverBookItem;

public interface BookMessageUseCase {

    void processCsvRow(String csvRow);

    void processSingleItem(NaverBookItem item);
}
