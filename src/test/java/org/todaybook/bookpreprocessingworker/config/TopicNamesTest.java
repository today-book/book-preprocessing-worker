package org.todaybook.bookpreprocessingworker.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TopicNamesTest {

    @Test
    @DisplayName("Explicit topics take precedence over prefix/env")
    void explicitTopicsOverridePrefixEnv() {
        AppKafkaProperties props = new AppKafkaProperties();
        props.setInputTopic("book.raw");
        props.setCsvInputTopic("csv-book.raw");
        props.setOutputTopic("book.parsed");
        props.setPrefix("prefix");
        props.setEnv("dev");

        TopicNames names = new TopicNames(props);

        assertThat(names.inputTopic()).isEqualTo("book.raw");
        assertThat(names.csvInputTopic()).isEqualTo("csv-book.raw");
        assertThat(names.outputTopic()).isEqualTo("book.parsed");
    }

    @Test
    @DisplayName("Prefix/env are applied when topics are absent")
    void prefixEnvAppliedWhenMissing() {
        AppKafkaProperties props = new AppKafkaProperties();
        props.setPrefix("svc");
        props.setEnv("dev");

        TopicNames names = new TopicNames(props);

        assertThat(names.inputTopic()).isEqualTo("svc.dev.");
        assertThat(names.csvInputTopic()).isEqualTo("svc.dev.");
        assertThat(names.outputTopic()).isEqualTo("svc.dev.");
    }
}
