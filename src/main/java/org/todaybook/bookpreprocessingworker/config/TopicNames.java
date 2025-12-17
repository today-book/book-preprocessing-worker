package org.todaybook.bookpreprocessingworker.config;

import org.springframework.util.StringUtils;

/**
 * Resolves Kafka topic names with optional prefix and environment suffix.
 * If explicit names are provided, they take precedence.
 */
public class TopicNames {

    private final String inputTopic;
    private final String csvInputTopic;
    private final String outputTopic;

    public TopicNames(AppKafkaProperties props) {
        this.inputTopic = resolve(props.getInputTopic(), props);
        this.csvInputTopic = resolve(
            StringUtils.hasText(props.getCsvInputTopic()) ? props.getCsvInputTopic() : props.getInputTopic(),
            props
        );
        this.outputTopic = resolve(props.getOutputTopic(), props);
    }

    private String resolve(String explicit, AppKafkaProperties props) {
        if (StringUtils.hasText(explicit)) return explicit;

        String logical = "";
        // If caller gave null/blank logical name, keep it blank so prefix/env still apply
        if (explicit != null) {
            logical = explicit;
        }

        String prefix = StringUtils.hasText(props.getPrefix()) ? props.getPrefix() : null;
        String env = StringUtils.hasText(props.getEnv()) ? props.getEnv() : null;

        StringBuilder sb = new StringBuilder();
        if (prefix != null) {
            sb.append(prefix).append('.');
        }
        if (env != null) {
            sb.append(env).append('.');
        }
        sb.append(logical);
        return sb.toString();
    }

    public String inputTopic() {
        return inputTopic;
    }

    public String csvInputTopic() {
        return csvInputTopic;
    }

    public String outputTopic() {
        return outputTopic;
    }
}
