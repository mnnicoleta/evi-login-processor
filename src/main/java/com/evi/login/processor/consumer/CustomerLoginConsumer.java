package com.evi.login.processor.consumer;

import com.evi.login.processor.mapper.LoginTrackingResultMapper;
import com.evi.login.processor.model.CustomerLoginEvent;
import com.evi.login.processor.service.LoginProcessingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.Map;

import static com.evi.login.processor.kafka.KafkaConstants.CONSUMER_CUSTOMER_LOGIN;
import static com.evi.login.processor.kafka.KafkaConstants.CUSTOMER_LOGIN;

/**
 * Reads from topic CUSTOMER_LOGIN, execute REST call and publish result
 */
@Slf4j
@Component
public class CustomerLoginConsumer {

    private final LoginProcessingService service;

    private final LoginTrackingResultMapper mapper;

    public CustomerLoginConsumer(LoginProcessingService service, LoginTrackingResultMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    /**
     * Consumes CustomerLoginEvent and if exists, basic auth
     *
     * @param customerLoginEvent CustomerLoginEvent
     * @param headers            headers
     */
//    @Transactional("kafkaTransactionManager")
    @KafkaListener(topics = CUSTOMER_LOGIN, groupId = CONSUMER_CUSTOMER_LOGIN, containerFactory = "listenerContainerFactoryCustomerLoginEvent")
    public void consume(@Payload CustomerLoginEvent customerLoginEvent, @Headers Map<String, Object> headers) {
        log.info("CustomerLoginEvent: " + customerLoginEvent.toString());
        service.processLogin(mapper.toResult(customerLoginEvent), headers).subscribe();
        //ack.acknowledge(); //safest for async side effect
    }

}