package com.evi.login.processor.consumer;

import com.evi.login.processor.entity.LoginTrackingResultEntity;
import com.evi.login.processor.mapper.LoginTrackingResultMapper;
import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.repository.LoginTrackingRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static com.evi.login.processor.kafka.KafkaConstants.*;

/**
 * Consumes LoginTrackingResultEvent, saves it into DB and publishes the result into next topic
 */
@Slf4j
@Component
public class CustomerLoginResultConsumer {

    private final LoginTrackingRepository repository;
    private final ReactiveKafkaProducerTemplate<String, LoginTrackingResultEntity> producer;
    private final LoginTrackingResultMapper mapper;

    public CustomerLoginResultConsumer(LoginTrackingRepository repository,
                                       ReactiveKafkaProducerTemplate<String, LoginTrackingResultEntity> producer,
                                       LoginTrackingResultMapper mapper) {
        this.repository = repository;
        this.producer = producer;
        this.mapper = mapper;
    }

    //    @Transactional("kafkaTransactionManager")
    @KafkaListener(topics = CUSTOMER_LOGIN_RESULT, groupId = CONSUMER_CUSTOMER_LOGIN_RESULT, containerFactory = "listenerContainerFactoryLoginTrackingResultEvent")
    public void consume(@Payload LoginTrackingResultEvent event) {

        repository.save(mapper.toEntity(event))
                .flatMap(result -> producer.send(LOGIN_TRACKING_RESULT, result.getCustomerId().toString(), result))
                .doOnNext(e -> log.debug("SAVED: " + e))
                .subscribe(); // save & publish only once
        //ack.acknowledge(); //safest for async side effect
    }
}