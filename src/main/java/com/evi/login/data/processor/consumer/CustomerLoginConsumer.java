package com.evi.login.data.processor.consumer;

import com.evi.login.data.processor.mapper.LoginTrackingResultMapper;
import com.evi.login.data.processor.model.CustomerLoginEvent;
import com.evi.login.data.processor.service.LoginProcessingService;
import org.mapstruct.factory.Mappers;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static com.evi.login.data.processor.constants.KafkaConstants.CONSUMER_CUSTOMER_LOGIN;
import static com.evi.login.data.processor.constants.KafkaConstants.CUSTOMER_LOGIN;

@Component
public class CustomerLoginConsumer {

    private final LoginProcessingService service;

    LoginTrackingResultMapper mapper = Mappers.getMapper(LoginTrackingResultMapper.class);

    public CustomerLoginConsumer(LoginProcessingService service) {
        this.service = service;
    }

    @KafkaListener(topics = CUSTOMER_LOGIN, groupId = CONSUMER_CUSTOMER_LOGIN)
    public void consume(@Payload CustomerLoginEvent customerLoginEvent) {

        service.processLogin(mapper.toResult(customerLoginEvent)).subscribe(); // reactive pipeline
    }

}