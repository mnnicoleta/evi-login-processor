package com.evi.login.processor.consumer;

import com.evi.login.processor.mapper.LoginTrackingResultMapper;
import com.evi.login.processor.model.CustomerLoginEvent;
import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.model.RequestResult;
import com.evi.login.processor.service.LoginProcessingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.Mockito.*;

class CustomerLoginConsumerTest {

    private LoginProcessingService service;
    private CustomerLoginConsumer consumer;
    private LoginTrackingResultMapper mapper;
    private KafkaReceiver<String, CustomerLoginEvent> kafkaReceiverCustomerLoginEvent;

    @BeforeEach
    void setup() {
        mapper = Mockito.mock(LoginTrackingResultMapper.class);
        service = mock(LoginProcessingService.class);
        kafkaReceiverCustomerLoginEvent = mock(KafkaReceiver.class);

        when(kafkaReceiverCustomerLoginEvent.receive()).thenReturn(Flux.empty());

        consumer = new CustomerLoginConsumer(service, mapper, kafkaReceiverCustomerLoginEvent);
    }

    @Test
    void processEventCallsServiceWithMappedEvent() {
        CustomerLoginEvent event = new CustomerLoginEvent(
                UUID.randomUUID(),
                "user1",
                "web",
                Instant.now(),
                UUID.randomUUID(),
                "127.0.0.1"
        );

        org.apache.kafka.common.header.Headers headers = mock(org.apache.kafka.common.header.Headers.class);

        LoginTrackingResultEvent mappedResult = new LoginTrackingResultEvent(
                event.customerId(),
                event.username(),
                event.client(),
                Instant.now(),
                UUID.randomUUID(),
                event.customerIp(),
                RequestResult.SUCCESSFUL
        );

        // Mock mapper and service
        when(mapper.toResult(event)).thenReturn(mappedResult);
        when(service.processLogin(mappedResult, headers)).thenReturn(Mono.empty());

        // Call the new processEvent method
        consumer.processEvent(event, headers).block(); // block() is fine in unit test

        verify(service, times(1)).processLogin(mappedResult, headers);
    }
}