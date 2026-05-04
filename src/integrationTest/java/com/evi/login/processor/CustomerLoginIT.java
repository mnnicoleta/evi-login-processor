package com.evi.login.processor;

import com.evi.login.processor.consumer.CustomerLoginConsumer;
import com.evi.login.processor.consumer.CustomerLoginResultConsumer;
import com.evi.login.processor.entity.LoginTrackingResultEntity;
import com.evi.login.processor.model.CustomerLoginEvent;
import com.evi.login.processor.model.RequestResult;
import com.evi.login.processor.repository.LoginTrackingRepository;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Description;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.evi.login.processor.config.kafka.KafkaConstants.*;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for the reactive approach
 * <p>
 * Testcontainers Kafka starts
 * ↓
 * Topic is created
 * ↓
 * Leader election finished
 * ↓
 * Spring Boot context starts
 * ↓
 * Kafka listeners start
 * ↓
 * Test sends message
 * ↓
 * Tests for different possible flows
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
@Testcontainers
@SpringBootTest
class CustomerLoginIT {

    private static final String TRACK_LOGING_PATH = "/trackLoging/.*";

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.8.7") //TODO: work with KRAFT
                    .asCompatibleSubstituteFor("apache/kafka")
    );

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15")
            .withDatabaseName("login_tracking_result")
            .withUsername("postgres")
            .withPassword("postgres");

    private static WireMockServer wireMockServer;

    @Autowired
    private LoginTrackingRepository repository;

    @Autowired
    private KafkaSender<String, CustomerLoginEvent> customerLoginEventSender;

    @Autowired
    private CustomerLoginConsumer customerLoginConsumer;

    @Autowired
    private CustomerLoginResultConsumer customerLoginResultConsumer;

    private CustomerLoginEvent sampleEventTest1;
    private CustomerLoginEvent sampleEventTest2;
    private CustomerLoginEvent sampleEventTest3;

    // ------------------------
    // Infra setup
    // ------------------------
    @BeforeAll
    static void setupInfra() throws Exception {
        wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wireMockServer.start();

        try (AdminClient adminClient = AdminClient.create(
                Collections.singletonMap("bootstrap.servers", kafka.getBootstrapServers())
        )) {
            adminClient.createTopics(List.of(
                    new NewTopic(CUSTOMER_LOGIN, 3, (short) 1), //min 3 partitions in prod
                    new NewTopic(CUSTOMER_LOGIN_RESULT, 3, (short) 1),
                    new NewTopic(LOGIN_TRACKING_RESULT, 3, (short) 1)
            )).all().get(30, TimeUnit.SECONDS);
        }
    }

    @AfterAll
    static void tearDown() {
        wireMockServer.stop();
    }

    @DynamicPropertySource
    static void registerProps(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);

        registry.add("login-tracker.service.url",
                () -> "http://localhost:" + wireMockServer.port());

        registry.add("customer-tracking.base-url",
                () -> "http://localhost:" + wireMockServer.port());

        String r2dbcUrl = String.format(
                "r2dbc:postgresql://%s:%d/%s",
                postgres.getHost(),
                postgres.getFirstMappedPort(),
                postgres.getDatabaseName()
        );

        registry.add("spring.r2dbc.url", () -> r2dbcUrl);
        registry.add("spring.r2dbc.username", postgres::getUsername);
        registry.add("spring.r2dbc.password", postgres::getPassword);
    }

    @BeforeEach
    void setupTest() {
        // we need to wait until kafka is ready
        await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    assertNotNull(customerLoginConsumer.getSubscription(), "CustomerLoginConsumer not subscribed");
                    assertNotNull(customerLoginResultConsumer.getSubscription(), "CustomerLoginResultConsumer not subscribed");
                });

        // clean DB to avoid flaky tests
        repository.deleteAll().block();

        sampleEventTest1 = new CustomerLoginEvent(
                UUID.randomUUID(),
                "johndoe",
                "web",
                Instant.now(),
                UUID.randomUUID(),
                "127.0.0.1"
        );

        sampleEventTest2 = new CustomerLoginEvent(
                UUID.randomUUID(),
                "johndoe2",
                "ios",
                Instant.now(),
                UUID.randomUUID(),
                "128.0.0.1"
        );

        sampleEventTest3 = new CustomerLoginEvent(
                UUID.randomUUID(),
                "johndoe2",
                "ios",
                Instant.now(),
                UUID.randomUUID(),
                "128.0.0.1"
        );
    }

    // ------------------------
    // Tests
    // ------------------------

    @Test
    @Description("successful login → HTTP 200 → SUCCESSFUL")
    void test1SuccessfulLogin() {

        wireMockServer.stubFor(post(urlPathMatching(TRACK_LOGING_PATH))
                .willReturn(aResponse().withStatus(200)));

        ProducerRecord<String, CustomerLoginEvent> producerRecord =
                new ProducerRecord<>(CUSTOMER_LOGIN,
                        sampleEventTest1.messageId().toString(),
                        sampleEventTest1);
        SenderRecord<String, CustomerLoginEvent, String> senderRecord =
                SenderRecord.create(producerRecord, sampleEventTest1.messageId().toString());

        String credentials = Base64.getEncoder()
                .encodeToString("user:password".getBytes());

        senderRecord.headers().add("Authorization",
                ("Basic " + credentials).getBytes(StandardCharsets.UTF_8));

        customerLoginEventSender.send(Mono.just(senderRecord)).blockLast();

        await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {

                    List<LoginTrackingResultEntity> list =
                            repository.findAll().collectList().block();

                    assertNotNull(list);
                    assertFalse(list.isEmpty());

                    LoginTrackingResultEntity entity = list.stream()
                            .filter(e -> e.getMessageId().equals(sampleEventTest1.messageId()))
                            .findFirst()
                            .orElseThrow();

                    assertEquals(RequestResult.SUCCESSFUL, entity.getRequestResult());
                    assertEquals(sampleEventTest1.username(), entity.getUsername());
                    assertEquals(sampleEventTest1.customerIp(), entity.getCustomerIp());
                    assertEquals(sampleEventTest1.timestamp(), entity.getTimestamp());
                    assertEquals(sampleEventTest1.client(), entity.getClient());
                    assertEquals(sampleEventTest1.customerId(), entity.getCustomerId());
                });
    }

    @Test
    @Description("missing Authorization → UNSUCCESSFUL")
    void test2MissingAuthorization() {

        ProducerRecord<String, CustomerLoginEvent> producerRecord =
                new ProducerRecord<>(CUSTOMER_LOGIN, sampleEventTest2.messageId().toString(), sampleEventTest2);

        SenderRecord<String, CustomerLoginEvent, String> senderRecord =
                SenderRecord.create(producerRecord, sampleEventTest2.messageId().toString());

        customerLoginEventSender.send(Mono.just(senderRecord)).blockLast();

        await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {

                    List<LoginTrackingResultEntity> list =
                            repository.findAll().collectList().block();

                    assertNotNull(list);
                    assertFalse(list.isEmpty());

                    LoginTrackingResultEntity entity = list.stream()
                            .filter(e -> e.getMessageId().equals(sampleEventTest2.messageId()))
                            .findFirst()
                            .orElseThrow();

                    assertEquals(RequestResult.UNSUCCESSFUL, entity.getRequestResult());
                    assertEquals(sampleEventTest2.username(), entity.getUsername());
                    assertEquals(sampleEventTest2.customerIp(), entity.getCustomerIp());
                    assertEquals(sampleEventTest2.timestamp(), entity.getTimestamp());
                    assertEquals(sampleEventTest2.client(), entity.getClient());
                    assertEquals(sampleEventTest2.customerId(), entity.getCustomerId());
                });
    }

    @Test
    @Description("HTTP failure → retries → UNSUCCESSFUL")
    void test3HttpFailure() {
        wireMockServer.stubFor(post(urlPathMatching(TRACK_LOGING_PATH))
                .willReturn(aResponse().withStatus(500)));

        ProducerRecord<String, CustomerLoginEvent> producerRecord =
                new ProducerRecord<>(CUSTOMER_LOGIN,
                        sampleEventTest3.messageId().toString(),
                        sampleEventTest3);

        SenderRecord<String, CustomerLoginEvent, String> senderRecord =
                SenderRecord.create(producerRecord, sampleEventTest3.messageId().toString());

        String credentials = Base64.getEncoder()
                .encodeToString("user:password".getBytes());

        senderRecord.headers().add("Authorization",
                ("Basic " + credentials).getBytes(StandardCharsets.UTF_8));

        customerLoginEventSender.send(Mono.just(senderRecord)).blockLast();

        await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {

                    List<LoginTrackingResultEntity> list =
                            repository.findAll().collectList().block();

                    assertNotNull(list);
                    assertFalse(list.isEmpty());

                    LoginTrackingResultEntity entity = list.stream()
                            .filter(e -> e.getMessageId().equals(sampleEventTest3.messageId()))
                            .findFirst()
                            .orElseThrow();

                    assertEquals(RequestResult.UNSUCCESSFUL, entity.getRequestResult());
                    assertEquals(sampleEventTest3.username(), entity.getUsername());
                    assertEquals(sampleEventTest3.customerIp(), entity.getCustomerIp());
                    assertEquals(sampleEventTest3.timestamp(), entity.getTimestamp());
                    assertEquals(sampleEventTest3.client(), entity.getClient());
                    assertEquals(sampleEventTest3.customerId(), entity.getCustomerId());
                });
    }

    @Test
    @Description("No duplicates should be saved. MessageId in db should be unique!")
    void test4EnsureNoDuplicatesGettingSaved() {
        final UUID messageId = UUID.randomUUID();
        final UUID customerId = UUID.randomUUID();

        CustomerLoginEvent sampleEventTest4 = new CustomerLoginEvent(
                customerId,
                "johndoe1",
                "ios",
                Instant.now(),
                messageId,
                "128.0.0.1"
        );

        CustomerLoginEvent sampleEventTest5 = new CustomerLoginEvent(
                customerId,
                "johndoe2",
                "android",
                Instant.now(),
                messageId,
                "128.0.0.2"
        );

        wireMockServer.stubFor(post(urlPathMatching(TRACK_LOGING_PATH))
                .willReturn(aResponse().withStatus(200)));

        ProducerRecord<String, CustomerLoginEvent> producerRecord =
                new ProducerRecord<>(CUSTOMER_LOGIN,
                        sampleEventTest4.messageId().toString(),
                        sampleEventTest4);
        SenderRecord<String, CustomerLoginEvent, String> senderRecord =
                SenderRecord.create(producerRecord, sampleEventTest4.messageId().toString());

        String credentials = Base64.getEncoder()
                .encodeToString("user:password".getBytes());

        senderRecord.headers().add("Authorization",
                ("Basic " + credentials).getBytes(StandardCharsets.UTF_8));

        customerLoginEventSender.send(Mono.just(senderRecord)).blockLast();

        await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {

                    List<LoginTrackingResultEntity> list =
                            repository.findAll().collectList().block();

                    assertNotNull(list);
                    assertFalse(list.isEmpty());

                    LoginTrackingResultEntity entity = list.stream()
                            .filter(e -> e.getMessageId().equals(sampleEventTest4.messageId()))
                            .findFirst()
                            .orElseThrow();

                    assertEquals(RequestResult.SUCCESSFUL, entity.getRequestResult());
                    assertEquals(sampleEventTest4.username(), entity.getUsername());
                    assertEquals(sampleEventTest4.customerIp(), entity.getCustomerIp());
                    assertEquals(sampleEventTest4.timestamp(), entity.getTimestamp());
                    assertEquals(sampleEventTest4.client(), entity.getClient());
                    assertEquals(sampleEventTest4.customerId(), entity.getCustomerId());
                });

        //sending second message event -> sampleEventTest5
        ProducerRecord<String, CustomerLoginEvent> producerRecord2 =
                new ProducerRecord<>(CUSTOMER_LOGIN,
                        sampleEventTest5.messageId().toString(),
                        sampleEventTest5);
        SenderRecord<String, CustomerLoginEvent, String> senderRecord2 =
                SenderRecord.create(producerRecord2, sampleEventTest5.messageId().toString());

        senderRecord2.headers().add("Authorization",
                ("Basic " + credentials).getBytes(StandardCharsets.UTF_8));

        customerLoginEventSender.send(Mono.just(senderRecord2)).blockLast();


        await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {

                    List<LoginTrackingResultEntity> list =
                            repository.findAll().collectList().block();

                    assertNotNull(list);
                    assertFalse(list.isEmpty());
                    assertEquals(1, list.size());

                    //still just sampleEventTest4 is saved in DB
                    LoginTrackingResultEntity entity = list.stream()
                            .filter(e -> e.getMessageId().equals(sampleEventTest4.messageId()))
                            .findFirst()
                            .orElseThrow();

                    assertEquals(RequestResult.SUCCESSFUL, entity.getRequestResult());
                    assertEquals(sampleEventTest4.username(), entity.getUsername());
                    assertEquals(sampleEventTest4.customerIp(), entity.getCustomerIp());
                    assertEquals(sampleEventTest4.timestamp(), entity.getTimestamp());
                    assertEquals(sampleEventTest4.client(), entity.getClient());
                    assertEquals(sampleEventTest4.customerId(), entity.getCustomerId());
                });
    }
}