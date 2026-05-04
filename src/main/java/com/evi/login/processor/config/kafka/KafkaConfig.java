package com.evi.login.processor.config.kafka;

import com.evi.login.processor.entity.LoginTrackingResultEntity;
import com.evi.login.processor.model.CustomerLoginEvent;
import com.evi.login.processor.model.LoginTrackingResultEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.evi.login.processor.config.kafka.KafkaConstants.*;

@Configuration
public class KafkaConfig {

    public static final String EVI_LOGIN_PROCESSOR_MODEL_PACKAGE = "com.evi.login.processor.model";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.producer.acks}")
    private String acks;
    @Value("${spring.kafka.producer.retries}")
    private int retries;
    @Value("${spring.kafka.producer.enable-idempotence}")
    private boolean idempotence;
    @Value("${spring.kafka.producer.max-in-flight-requests-per-connection}")
    private int maxInFlight;
    @Value("${spring.kafka.producer.linger-ms}")
    private int lingerMs;
    @Value("${spring.kafka.producer.batch-size}")
    private int batchSize;
    @Value("${spring.kafka.producer.compression-type}")
    private String compressionType;

    // -----------------------
    // Reactive Producers
    // -----------------------
    @Bean
    public KafkaSender<String, CustomerLoginEvent> customerLoginEventSender() {
        return createKafkaSender(CUSTOMER_LOGIN);
    }

    @Bean
    public KafkaSender<String, LoginTrackingResultEvent> loginTrackingResultEventSender() {
        return createKafkaSender(LOGIN_TRACKING_RESULT_PRODUCER);
    }

    @Bean
    public KafkaSender<String, LoginTrackingResultEntity> loginTrackingResultEntitySender() {
        return createKafkaSender(LOGIN_TRACKING_RESULT_ENTITY_PRODUCER);
    }

    private <T> KafkaSender<String, T> createKafkaSender(String clientId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Reliability & exactly-once
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        // Tuning
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);

        SenderOptions<String, T> senderOptions = SenderOptions.create(props);

        return KafkaSender.create(senderOptions);
    }

    @Bean
    public KafkaReceiver<String, CustomerLoginEvent> kafkaReceiverCustomerLoginEvent() {
        return KafkaReceiver.create(customerLoginReceiverOptions());
    }

    @Bean
    public ReceiverOptions<String, CustomerLoginEvent> customerLoginReceiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_CUSTOMER_LOGIN);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, CustomerLoginEvent.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, EVI_LOGIN_PROCESSOR_MODEL_PACKAGE);

        return ReceiverOptions.<String, CustomerLoginEvent>create(props)
                .subscription(Collections.singleton(CUSTOMER_LOGIN));
    }

    @Bean
    public KafkaReceiver<String, LoginTrackingResultEvent> kafkaReceiverLoginTrackingResultEvent() {
        return KafkaReceiver.create(customerLoginResultReceiverOptions());
    }

    @Bean
    public ReceiverOptions<String, LoginTrackingResultEvent> customerLoginResultReceiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_CUSTOMER_LOGIN_RESULT); // separate group!
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_DOC, LoginTrackingResultEvent.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, LoginTrackingResultEvent.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, EVI_LOGIN_PROCESSOR_MODEL_PACKAGE);

        return ReceiverOptions.<String, LoginTrackingResultEvent>create(props)
                .subscription(Collections.singleton(CUSTOMER_LOGIN_RESULT));
    }
}