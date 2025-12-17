package org.todaybook.bookpreprocessingworker.infrastructure.kafka.listener;

public interface BookMessageListener {

    void onMessage(String payload) throws Exception;
}
