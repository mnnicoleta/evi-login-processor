package com.evi.login.processor;

import com.evi.login.processor.consumer.CustomerLoginConsumer;
import com.evi.login.processor.consumer.CustomerLoginResultConsumer;
import com.evi.login.processor.entity.LoginTrackingResultEntity;
import com.evi.login.processor.model.CustomerLoginEvent;
import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.model.RequestResult;
import com.evi.login.processor.repository.LoginTrackingRepository;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.evi.login.processor.constants.KafkaConstants.CUSTOMER_LOGIN;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
@SpringBootTest(properties = {
        "login-tracker.service.url=http://localhost:8081", // placeholder, overridden dynamically
        "spring.main.allow-bean-definition-overriding=true"
})
@Profile("local")
class CustomerLoginIT {
    // ------------------------
    // Kafka Testcontainer
    // ------------------------
    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.8.7") // latest stable
                    .asCompatibleSubstituteFor("apache/kafka")
    );

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15")
            .withDatabaseName("login_tracking_result")
            .withUsername("postgres")
            .withPassword("postgres");

    // ------------------------
    // WireMock Test Server
    // ------------------------
    private static WireMockServer wireMockServer;

    @Autowired
    private CustomerLoginConsumer customerLoginConsumer;

    @Autowired
    private CustomerLoginResultConsumer customerLoginResultConsumer;

    @Autowired
    private LoginTrackingRepository repository;

    private CustomerLoginEvent sampleEvent;
    private LoginTrackingResultEvent expectedResult;

    //to produce events
    @Autowired
    private ReactiveKafkaProducerTemplate<String, CustomerLoginEvent> kafkaTemplate;

    // ------------------------
    // WireMock setup
    // ------------------------
    @BeforeAll
    static void startWireMock() {
        wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wireMockServer.start();
    }

    @AfterAll
    static void stopWireMock() {
        wireMockServer.stop();
    }

    // ------------------------
    // Dynamic properties for Kafka + WireMock
    // ------------------------
    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("login-tracker.service.url", () -> "http://localhost:" + wireMockServer.port());

        // R2DBC
        String r2dbcUrl = String.format(
                "r2dbc:postgresql://%s:%d/%s",
                postgres.getHost(),                // container host
                postgres.getFirstMappedPort(),     // mapped port
                postgres.getDatabaseName()         // database name
        );
        registry.add("spring.r2dbc.url", () -> r2dbcUrl);
        registry.add("spring.r2dbc.username", postgres::getUsername);
        registry.add("spring.r2dbc.password", postgres::getPassword);

        // JDBC (if needed by JPA)
        String jdbcUrl = String.format(
                "jdbc:postgresql://%s:%d/%s",
                postgres.getHost(),
                postgres.getFirstMappedPort(),
                postgres.getDatabaseName()
        );
        registry.add("spring.datasource.url", () -> jdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    // ------------------------
    // Setup sample event before each test
    // ------------------------
    @BeforeEach
    void setUp() {
        UUID customerId = UUID.randomUUID();
        sampleEvent = new CustomerLoginEvent(
                customerId,
                "johndoe",
                "web",
                Instant.now(),
                UUID.randomUUID(),
                "127.0.0.1"
        );

        expectedResult = new LoginTrackingResultEvent(
                sampleEvent.customerId(),
                sampleEvent.username(),
                sampleEvent.client(),
                sampleEvent.timestamp(),
                sampleEvent.messageId(),
                sampleEvent.customerIp(),
                RequestResult.SUCCESSFUL
        );
    }

    // ------------------------
    // Positive test: successful login
    // ------------------------
    @Test
    void testCustomerLogin_Success() {
        // pretend we have a 200 HTTP for the rest CALL
        wireMockServer.stubFor(post(urlPathMatching("/trackLoging/.*"))
                .willReturn(aResponse().withStatus(200)));

        // send event, in order to really test the consumer 1
        ProducerRecord<String, CustomerLoginEvent> record =
                new ProducerRecord<>(CUSTOMER_LOGIN, sampleEvent.customerId().toString(), sampleEvent);
        String credentials = Base64.getEncoder()
                .encodeToString("user:password".getBytes());
        record.headers().add(
                "Authorization",
                ("Basic " + credentials).getBytes(StandardCharsets.UTF_8)
        );
        kafkaTemplate.send(record);


        await().atMost(5, TimeUnit.SECONDS)
                .pollInterval(300, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> {
                    List<LoginTrackingResultEntity> list = repository.findAll().collectList().block();
                    assertEquals(1, list.size());
                    LoginTrackingResultEntity entity = list.getFirst();
                    assertEquals(RequestResult.SUCCESSFUL, entity.getRequestResult());
                    assertEquals(sampleEvent.customerIp(), entity.getCustomerIp());
                });

    }

    // ------------------------f
    // Negative test: no Authorization header
    // ------------------------
    @Test
    void testCustomerLogin_NoAuthorization() {
        // No WireMock stub needed, service will return UNSUCCESSFUL
        Headers headers = new RecordHeaders(); // empty headers

        // Consume event
        customerLoginConsumer.consume(sampleEvent, headers);

        StepVerifier.create(Mono.delay(java.time.Duration.ofSeconds(1)))
                .expectNextCount(1)
                .verifyComplete();

        // Assert repository contains UNSUCCESSFUL result
        StepVerifier.create(repository.findAll().collectList())
                .assertNext(list -> {
                    assertEquals(1, list.size());
                    LoginTrackingResultEntity entity = list.getFirst();
                    assertEquals(RequestResult.UNSUCCESSFUL, entity.getRequestResult());
                    assertEquals(sampleEvent.customerIp(), entity.getCustomerIp());
                })
                .verifyComplete();
    }
}