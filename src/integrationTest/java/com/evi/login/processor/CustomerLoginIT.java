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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.evi.login.processor.kafka.KafkaConstants.*;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
@SpringBootTest
@Profile("local")
class CustomerLoginIT {
    // ------------------------
    // Kafka Testcontainer
    // ------------------------
    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.8.7")
                    .asCompatibleSubstituteFor("apache/kafka")
            /**
             * The image confluentinc/cp-kafka provides:
             * Kafka broker (Apache Kafka 3.9.1)
             * Confluent packaging/config
             * JDK + utilities required for running Kafka in containers.
             */
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
    KafkaListenerEndpointRegistry registry;
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
    static void startWireMock() throws ExecutionException, InterruptedException, TimeoutException {
        wireMockServer = new WireMockServer(WireMockConfiguration.options().dynamicPort());
        wireMockServer.start();

        //creating topics before (topic exists, leader election finished, consumers can connect)
        try (AdminClient adminClient = AdminClient.create(
                Collections.singletonMap(
                        "bootstrap.servers",
                        kafka.getBootstrapServers()
                )
        )) {
            // 3 partitions, replication factor 1 in dev !! 3 in prod (depending on how much durability you want)
            NewTopic topic1 = new NewTopic(CUSTOMER_LOGIN, 3, (short) 1);
            NewTopic topic2 = new NewTopic(CUSTOMER_LOGIN_RESULT, 3, (short) 1);
            NewTopic topic3 = new NewTopic(LOGIN_TRACKING_RESULT, 3, (short) 1);

            adminClient.createTopics(List.of(topic1, topic2, topic3))
                    .all()
                    .get(30, TimeUnit.SECONDS);
        }
    }

//    Testcontainers Kafka starts
//        ↓
//    Topic is created
//        ↓
//    Leader election finished
//        ↓
//    Spring Boot context starts
//        ↓
//    Kafka listeners start
//        ↓
//    Test sends message

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

        registry.add("customer-tracking.base-url", () -> "http://localhost:" + wireMockServer.port());
    }

    @BeforeEach
    void waitForKafka() {
        registry.getListenerContainers().forEach(container ->
                await().until(container::isRunning)
        );
    }

    // ------------------------
    // Setup sample event before each test
    // ------------------------
    @BeforeEach
    void setUp() {
        UUID customerId = UUID.randomUUID();
        UUID messageId = UUID.randomUUID();
        Instant timestamp = Instant.now();

        sampleEvent = new CustomerLoginEvent(
                customerId,
                "johndoe",
                "web",
                timestamp,
                messageId,
                "127.0.0.1"
        );

        expectedResult = new LoginTrackingResultEvent(
                sampleEvent.getCustomerId(),
                sampleEvent.getUsername(),
                sampleEvent.getClient(),
                sampleEvent.getTimestamp(),
                sampleEvent.getMessageId(),
                sampleEvent.getCustomerIp(),
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
                new ProducerRecord<>(CUSTOMER_LOGIN, sampleEvent.getCustomerId().toString(), sampleEvent);

        String credentials = Base64.getEncoder()
                .encodeToString("user:password".getBytes());
        record.headers().add("Authorization",
                ("Basic " + credentials).getBytes(StandardCharsets.UTF_8)
        );
        kafkaTemplate.send(record).block();

        //wait for the consumer1 (REST call 3 retries + publish result pe TOPIC2) then C3 save in DB and publish in TOPIC3
        await()
                .atMost(40, TimeUnit.SECONDS)
                .untilAsserted(() -> {

                    List<LoginTrackingResultEntity> list = repository.findAll().collectList().block();
                    assertTrue(!list.isEmpty());

                    LoginTrackingResultEntity entity = list.getFirst();
                    assertEquals(RequestResult.SUCCESSFUL, entity.getRequestResult());
                    assertEquals(sampleEvent.getCustomerIp(), entity.getCustomerIp());
                });
    }

    @Test
    void testCustomerLogin_NoAuthorization_Unsuccessfull() {
        // No WireMock stub needed, service will return UNSUCCESSFUL
        ProducerRecord<String, CustomerLoginEvent> record =
                new ProducerRecord<>(CUSTOMER_LOGIN, sampleEvent.getCustomerId().toString(), sampleEvent);

        kafkaTemplate.send(record).block();

        //wait for the consumer1 (REST call 3 retries + publish result pe TOPIC2) then C3 save in DB and publish in TOPIC3
        await()
                .atMost(40, TimeUnit.SECONDS)
                .untilAsserted(() -> {

                    List<LoginTrackingResultEntity> list = repository.findAll().collectList().block();
                    assertTrue(!list.isEmpty());

                    LoginTrackingResultEntity entity = list.getFirst();
                    assertEquals(RequestResult.UNSUCCESSFUL, entity.getRequestResult());
                    assertEquals(sampleEvent.getCustomerId(), entity.getCustomerId());
                });
    }

    @Test
    void testCustomerLogin_RequestFails_Unsuccessfull() {
        // No WireMock stub needed, service will return UNSUCCESSFUL
        ProducerRecord<String, CustomerLoginEvent> record =
                new ProducerRecord<>(CUSTOMER_LOGIN, sampleEvent.getCustomerId().toString(), sampleEvent);

        String credentials = Base64.getEncoder()
                .encodeToString("user:password".getBytes());
        record.headers().add("Authorization",
                ("Basic " + credentials).getBytes(StandardCharsets.UTF_8)
        );
        kafkaTemplate.send(record).block();

        //wait for the consumer1 (REST call 3 retries + publish result pe TOPIC2) then C3 save in DB and publish in TOPIC3
        await()
                .atMost(40, TimeUnit.SECONDS)
                .untilAsserted(() -> {

                    List<LoginTrackingResultEntity> list = repository.findAll().collectList().block();
                    assertTrue(!list.isEmpty());

                    LoginTrackingResultEntity entity = list.getFirst();
                    assertEquals(RequestResult.UNSUCCESSFUL, entity.getRequestResult());
                    assertEquals(sampleEvent.getCustomerId(), entity.getCustomerId());
                });
    }
}