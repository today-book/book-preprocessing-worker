package org.todaybook.bookpreprocessingworker.infrastructure.kafka.listener;

public interface BookMessageListener<T> {

    void onMessage(T payload);
}
