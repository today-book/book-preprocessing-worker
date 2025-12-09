package org.todaybook.bookpreprocessingworker.application.kafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@DirtiesContext // 테스트 종료 후 컨텍스트를 리로드하여 다른 테스트에 영향을 주지 않도록 설정
@EmbeddedKafka(partitions = 1, topics = {"book.raw"}, ports = {9999}) // 포트를 9092로 고정하거나, 랜덤 포트 사용 시 properties 설정 필요
@SpringBootTest(properties = {
    // 1. 임베디드 카프카가 실행된 주소(브로커)를 Spring Boot 설정에 주입
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    // 2. 테스트 환경에서 사용할 토픽 이름 명시 (application.yml 설정 덮어쓰기)
    "app.kafka.input-topic=book.raw"
})
class BookSearchResponseProducerTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.input-topic}")
    private String topic;

    @Test
    @DisplayName("네이버 검색 결과 JSON을 Kafka로 전송한다 (수동 트리거)")
    void sendMockNaverSearchResponse() {
        // 우리가 만든 '완벽한 한 줄' JSON
        String jsonPayload = """
            {"lastBuildDate":"Fri, 05 Dec 2025 09:50:46 +0900","total":131,"start":1,"display":100,"items":[{"title":"르몽드 디플로마티크(Le Monde Diplomatique)(한국어판)(2025년 12월호) (207호)","link":"https://search.shopping.naver.com/book/catalog/57944061835","image":"https://shopping-phinf.pstatic.net/main_5794406/57944061835.20251130072856.jpg","author":"브누아 브레빌^르몽드디플로마티크 편집부","discount":"17100","publisher":"르몽드디플로마티크","pubdate":"20251128","isbn":"9791192618944","description":"\\"프랑스《르몽드》의 자매지로 전세계 27개 언어, 84개 국제판으로 발행되는 월간지\\n\\n‘진실을, 모든 진실을, 오직 진실만을 말하라’라는 언론관으로 유명한 프랑스 일간지 《르몽드(Le Monde)》의 자매지이자 국제관계 전문 시사지인 《르몽드 디플로마티크》는 국제 이슈에 대한 깊이 있는 분석과 참신한 문제제기로 인류 보편의 가치인 인권, 민주주의, 평등박애주의, 환경보전, 반전평화 등을 옹호하는 대표적인 독립 대안언론이다. 미국의 석학 노암 촘스키가 ‘세계의 창’이라고 부른 《르몽드 디플로마티크》는 신자유주의 세계화의 폭력성을 드러내는 데에서 더 나아가 ‘아탁(ATTAC)’과 ‘세계사회포럼(WSF, World Social Forum)’ 같은 대안세계화를 위한 NGO 활동과, 거대 미디어의 신자유주의적 논리와 횡포를 저지하는 지구적인 미디어 감시기구 활동에 역점을 두는 등 적극적으로 현실사회운동에 참여하고 있다. 발행인 겸 편집인 세르주 알리미는 “우리가 던지는 질문은 간단하다. 세계로 향한 보편적 이익을 지속적으로 추구하면서 잠비아 광부들과 중국 해군, 라트비아 사회를 다루는 데 두 바닥의 지면을 할애하는 이가 과연 우리 말고 누가 있겠는가? 우리의 필자는 세기의 만찬에 초대받은 적도 없고 제약업계의 로비에 휘말리지도 않으며 거대 미디어들과 모종의 관계에 있지도 않다”라고 하면서 신자유주의적 질서에 맞서는 편집진의 각오를 밝힌 바 있다.\\n한국 독자들 사이에서 ‘르디플로’라는 애칭으로 불리는 《르몽드 디플로마티크》는 2014년 현재 27개 언어, 84개 국제판으로 240만 부 이상 발행되고 있으며, 국내에서도 2008년 10월 재창간을 통해 한국 독자들과 만나고 있다(www.ilemonde.com 참조). 이 잡지에는 이냐시오 라모네, 레지스 드브레, 앙드레 고르즈, 장 셰노, 리카르도 페트렐라, 노암 촘스키, 자크 데리다, 에릭 홉스봄, 슬라보예 지젝, 알랭 바디우 등 세계 석학과 유명 필진이 글을 기고함으로써 다양한 의제를 깊이 있게 전달하고 있다.\\""},{"title":"르몽드 디플로마티크(Le Monde Diplomatique)(한국어판)(2025년 11월호) (206호)","link":"https://search.shopping.naver.com/book/catalog/57453366931","image":"https://shopping-phinf.pstatic.net/main_5745336/57453366931.20251101083800.jpg","author":"브누아 브레빌","discount":"17100","publisher":"주식회사 르몽드디플로마티크","pubdate":"20251030","isbn":"9791192618906","description":"프랑스《르몽드》의 자매지로 전세계 27개 언어, 84개 국제판으로 발행되는 월간지\\n\\n‘진실을, 모든 진실을, 오직 진실만을 말하라’라는 언론관으로 유명한 프랑스 일간지 《르몽드(Le Monde)》의 자매지이자 국제관계 전문 시사지인 《르몽드 디플로마티크》는 국제 이슈에 대한 깊이 있는 분석과 참신한 문제제기로 인류 보편의 가치인 인권, 민주주의, 평등박애주의, 환경보전, 반전평화 등을 옹호하는 대표적인 독립 대안언론이다. 미국의 석학 노암 촘스키가 ‘세계의 창’이라고 부른 《르몽드 디플로마티크》는 신자유주의 세계화의 폭력성을 드러내는 데에서 더 나아가 ‘아탁(ATTAC)’과 ‘세계사회포럼(WSF, World Social Forum)’ 같은 대안세계화를 위한 NGO 활동과, 거대 미디어의 신자유주의적 논리와 횡포를 저지하는 지구적인 미디어 감시기구 활동에 역점을 두는 등 적극적으로 현실사회운동에 참여하고 있다. 발행인 겸 편집인 세르주 알리미는 “우리가 던지는 질문은 간단하다. 세계로 향한 보편적 이익을 지속적으로 추구하면서 잠비아 광부들과 중국 해군, 라트비아 사회를 다루는 데 두 바닥의 지면을 할애하는 이가 과연 우리 말고 누가 있겠는가? 우리의 필자는 세기의 만찬에 초대받은 적도 없고 제약업계의 로비에 휘말리지도 않으며 거대 미디어들과 모종의 관계에 있지도 않다”라고 하면서 신자유주의적 질서에 맞서는 편집진의 각오를 밝힌 바 있다.\\n한국 독자들 사이에서 ‘르디플로’라는 애칭으로 불리는 《르몽드 디플로마티크》는 2014년 현재 27개 언어, 84개 국제판으로 240만 부 이상 발행되고 있으며, 국내에서도 2008년 10월 재창간을 통해 한국 독자들과 만나고 있다(www.ilemonde.com 참조). 이 잡지에는 이냐시오 라모네, 레지스 드브레, 앙드레 고르즈, 장 셰노, 리카르도 페트렐라, 노암 촘스키, 자크 데리다, 에릭 홉스봄, 슬라보예 지젝, 알랭 바디우 등 세계 석학과 유명 필진이 글을 기고함으로써 다양한 의제를 깊이 있게 전달하고 있다."}]}
            """;

        System.out.println(">>> Sending Test Payload to Topic: " + topic);

        try {
            // .get(10, TimeUnit.SECONDS)를 사용하여 동기적으로 전송 완료를 기다립니다.
            // 테스트 코드에서는 Thread.sleep 대신 이 방식을 권장합니다.
            var result = kafkaTemplate.send(topic, jsonPayload).get(10, TimeUnit.SECONDS);

            System.out.println("✅ SUCCESS! Offset: " + result.getRecordMetadata().offset());

        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            System.err.println("❌ FAILURE! " + e.getMessage());
            throw new RuntimeException("Kafka 전송 실패", e);
        }
    }
}