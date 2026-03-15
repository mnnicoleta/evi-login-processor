package com.evi.login.processor.consumer;

import com.evi.login.processor.mapper.LoginTrackingResultMapper;
import com.evi.login.processor.model.CustomerLoginEvent;
import com.evi.login.processor.service.LoginProcessingService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static com.evi.login.processor.constants.KafkaConstants.CONSUMER_CUSTOMER_LOGIN;
import static com.evi.login.processor.constants.KafkaConstants.CUSTOMER_LOGIN;

@Component
public class CustomerLoginConsumer {

    private final LoginProcessingService service;

    private final LoginTrackingResultMapper mapper;

    public CustomerLoginConsumer(LoginProcessingService service, LoginTrackingResultMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    @KafkaListener(topics = CUSTOMER_LOGIN, groupId = CONSUMER_CUSTOMER_LOGIN)
    public void consume(@Payload CustomerLoginEvent customerLoginEvent, @Headers org.apache.kafka.common.header.Headers headers) {

        service.processLogin(mapper.toResult(customerLoginEvent), headers).subscribe(); // reactive pipeline
    }

}