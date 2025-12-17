package org.todaybook.bookpreprocessingworker;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
    // Use local Kafka (9092) or override via KAFKA_BOOTSTRAP_SERVERS for CI
    "spring.kafka.bootstrap-servers=${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}"
})
class BookpreprocessingworkerApplicationTests {

	@Test
	void contextLoads() {
	}

}
