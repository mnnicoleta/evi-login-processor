package com.evi.login.data.processor.config;

import com.evi.login.data.processor.entity.LoginTrackingResultEntity;
import com.evi.login.data.processor.model.CustomerLoginEvent;
import com.evi.login.data.processor.model.LoginTrackingResultEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.evi.login.data.processor.constants.KafkaConstants.*;

@Profile({"local"})
@Configuration
public class KafkaConfig {

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

    // Producers
    @Bean
    public ReactiveKafkaProducerTemplate<String, LoginTrackingResultEvent> loginTrackingResultEventProducerTemplate() {
        return createProducer(LOGIN_TRACKING_RESULT_PRODUCER, LoginTrackingResultEvent.class);
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, LoginTrackingResultEntity> loginTrackingResultEventEntityProducerTemplate() {
        return createProducer(LOGIN_TRACKING_RESULT_ENTITY_PRODUCER, LoginTrackingResultEntity.class);
    }

    private <T> ReactiveKafkaProducerTemplate<String, T> createProducer(String clientId, Class<T> valueClass) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotence);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlight);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
        props.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);

        SenderOptions<String, T> senderOptions = SenderOptions.create(props);
        return new ReactiveKafkaProducerTemplate<>(senderOptions);
    }

    // Consumer for CustomerLogin
    @Bean
    public ConsumerFactory<String, CustomerLoginEvent> consumerCustomerLoginFactory() {
        JsonDeserializer<CustomerLoginEvent> deserializer = new JsonDeserializer<>(CustomerLoginEvent.class);
        deserializer.addTrustedPackages("*");

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_CUSTOMER_LOGIN);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), deserializer);
    }

    // Consumer for CustomerLoginResultEvent
    @Bean
    public ReceiverOptions<String, CustomerLoginEvent> customerLoginReceiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_CUSTOMER_LOGIN);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        return ReceiverOptions.<String, CustomerLoginEvent>create(props)
                .subscription(Collections.singleton(CUSTOMER_LOGIN));
    }

    @Bean
    public ReceiverOptions<String, LoginTrackingResultEvent> customerLoginResultReceiverOptions() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_CUSTOMER_LOGIN);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        return ReceiverOptions.<String, LoginTrackingResultEvent>create(props)
                .subscription(Collections.singleton(CUSTOMER_LOGIN_RESULT));
    }

//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, CustomerLogin> kafkaListenerContainerFactory() {
//        ConcurrentKafkaListenerContainerFactory<String, CustomerLogin> factory =
//                new ConcurrentKafkaListenerContainerFactory<>();
//        factory.setConsumerFactory(consumerFactory());
//        factory.setConcurrency(3);
//        return factory;
//    }
}