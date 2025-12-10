package org.todaybook.bookpreprocessingworker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka")
public class AppKafkaProperties {

    private String inputTopic;
    private String outputTopic;

    /**
     * Kafka에서 사용될 입력 토픽의 이름을 반환한다.
     *
     * @return 설정된 입력 토픽 이름. 설정되지 않은 경우 {@code null}.
     */
    public String getInputTopic() {
        return inputTopic;
    }

    /**
     * Kafka 입력 토픽의 이름을 설정합니다.
     *
     * @param inputTopic 설정할 입력 토픽 이름
     */
    public void setInputTopic(String inputTopic) {
        this.inputTopic = inputTopic;
    }

    /**
     * Kafka 출력 토픽의 이름을 제공한다.
     *
     * @return 출력 토픽의 이름
     */
    public String getOutputTopic() {
        return outputTopic;
    }

    /**
     * Kafka 출력 토픽의 이름을 설정한다.
     *
     * @param outputTopic Kafka 출력 토픽의 이름
     */
    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }
}