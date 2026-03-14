package com.evi.login.data.processor.consumer;

import com.evi.login.data.processor.entity.LoginTrackingResultEntity;
import com.evi.login.data.processor.mapper.LoginTrackingResultMapper;
import com.evi.login.data.processor.model.LoginTrackingResultEvent;
import com.evi.login.data.processor.repository.LoginTrackingRepository;
import org.mapstruct.factory.Mappers;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static com.evi.login.data.processor.constants.KafkaConstants.CONSUMER_CUSTOMER_LOGIN_RESULT;
import static com.evi.login.data.processor.constants.KafkaConstants.CUSTOMER_LOGIN_RESULT;

@Component
public class CustomerLoginResultConsumer {

    private final LoginTrackingRepository repository;
    private final ReactiveKafkaProducerTemplate<String, LoginTrackingResultEntity> producer;
    LoginTrackingResultMapper mapper = Mappers.getMapper(LoginTrackingResultMapper.class);

    public CustomerLoginResultConsumer(LoginTrackingRepository repository,
                                       ReactiveKafkaProducerTemplate<String, LoginTrackingResultEntity> producer) {
        this.repository = repository;
        this.producer = producer;
    }

    @KafkaListener(topics = CUSTOMER_LOGIN_RESULT, groupId = CONSUMER_CUSTOMER_LOGIN_RESULT)
    public void consume(@Payload LoginTrackingResultEvent event) {

        repository.save(mapper.toEntity(event))
                .flatMap(result -> producer.send(CUSTOMER_LOGIN_RESULT, result.getCustomerIp(), result))
                .subscribe(); // save & publish only once
    }
}