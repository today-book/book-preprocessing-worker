package org.todaybook.bookpreprocessingworker.application.kafka;

public interface BookMessageListener {

    void onMessage(String payload) throws Exception;
}
