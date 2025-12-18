package org.todaybook.bookpreprocessingworker;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import org.todaybook.bookpreprocessingworker.domain.model.Book;

/**
 * Local 프로파일 통합 테스트
 *
 * <p>이 테스트는 docker-compose.local.yml로 실행된 외부 인프라(Kafka)에
 * 실제로 연결하여 애플리케이션이 정상적으로 동작하는지 검증합니다.</p>
 *
 * <h3>사전 조건</h3>
 * <pre>
 * docker-compose -f docker-compose.local.yml up -d
 * </pre>
 *
 * <h3>환경변수 주입 방법</h3>
 * <p>이 테스트는 {@code @BeforeAll}에서 .env.local 파일을 읽어 시스템 프로퍼티로 설정합니다.
 * 또는 다음 방법들을 사용할 수 있습니다:</p>
 * <ul>
 *   <li>IDE Run Configuration에서 환경변수 설정</li>
 *   <li>Gradle: {@code ./gradlew test --tests "*LocalProfileIntegrationTest" -Dspring.profiles.active=local}</li>
 *   <li>CI/CD: GitHub Actions secrets나 환경변수로 주입</li>
 * </ul>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("local")
class LocalProfileIntegrationTest {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    @Qualifier("bookKafkaTemplate")
    private KafkaTemplate<String, Book> kafkaTemplate;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.input-topic}")
    private String inputTopic;

    @Value("${app.kafka.output-topic}")
    private String outputTopic;

    /**
     * .env.local 파일에서 환경변수를 읽어 시스템 프로퍼티로 설정합니다.
     * Spring이 ${...} 플레이스홀더를 해석할 때 시스템 프로퍼티를 참조합니다.
     */
    @BeforeAll
    static void loadEnvFile() throws IOException {
        Path envPath = Path.of(".env.local");
        if (Files.exists(envPath)) {
            Files.readAllLines(envPath).stream()
                    .filter(line -> !line.isBlank() && !line.startsWith("#"))
                    .map(line -> line.split("=", 2))
                    .filter(parts -> parts.length == 2)
                    .forEach(parts -> {
                        String key = parts[0].trim();
                        String value = parts[1].trim();
                        if (System.getProperty(key) == null) {
                            System.setProperty(key, value);
                        }
                    });
            System.out.println(">>> [TEST] .env.local 파일 로드 완료");
        } else {
            System.out.println(">>> [TEST] .env.local 파일이 없습니다. 기본 설정을 사용합니다.");
        }
    }

    @Test
    @DisplayName("Spring Application Context가 local 프로파일로 정상 로드되어야 한다")
    void contextLoads() {
        assertThat(applicationContext).isNotNull();

        String[] activeProfiles = applicationContext.getEnvironment().getActiveProfiles();
        assertThat(activeProfiles).contains("local");

        System.out.println(">>> [TEST] Active Profiles: " + String.join(", ", activeProfiles));
    }

    @Test
    @DisplayName("KafkaTemplate 빈이 정상적으로 주입되어야 한다")
    void kafkaTemplateBeanExists() {
        assertThat(kafkaTemplate).isNotNull();
        System.out.println(">>> [TEST] KafkaTemplate 빈 주입 성공");
    }

    @Test
    @DisplayName("Kafka 브로커에 연결할 수 있어야 한다")
    void kafkaBrokerIsReachable() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5000);

        try (AdminClient adminClient = AdminClient.create(props)) {
            ListTopicsResult topics = adminClient.listTopics();
            Set<String> topicNames = topics.names().get(10, TimeUnit.SECONDS);

            assertThat(topicNames).isNotNull();
            System.out.println(">>> [TEST] Kafka 연결 성공! 토픽 목록: " + topicNames);
        }
    }

    @Test
    @DisplayName("Kafka로 메시지를 전송할 수 있어야 한다")
    void canSendMessageToKafka() throws ExecutionException, InterruptedException, TimeoutException {
        String testMessage = """
            {
              "title": "Integration Test Book",
              "author": "Integration Author",
              "isbn": "9781234567890",
              "description": "Integration test description long enough for validation."
            }
            """;

        KafkaTemplate<String, String> rawTemplate = new KafkaTemplate<>(
            new DefaultKafkaProducerFactory<>(producerProps(), new StringSerializer(), new StringSerializer())
        );

        rawTemplate.send(inputTopic, "9781234567890", testMessage).get(10, TimeUnit.SECONDS);

        System.out.println(">>> [TEST] 메시지 전송 성공: topic=" + inputTopic + ", message=" + testMessage);
    }

    @Test
    @DisplayName("application-local.yml 설정값이 올바르게 로드되어야 한다")
    void configurationPropertiesAreLoaded() {
        assertThat(bootstrapServers).isEqualTo("localhost:9092");
        assertThat(inputTopic).isEqualTo("book.raw.naver");
        assertThat(outputTopic).isEqualTo("book.parsed");

        System.out.println(">>> [TEST] 설정값 확인:");
        System.out.println("    - bootstrap-servers: " + bootstrapServers);
        System.out.println("    - input-topic: " + inputTopic);
        System.out.println("    - output-topic: " + outputTopic);
    }

    private Map<String, Object> producerProps() {
        return Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );
    }
}
