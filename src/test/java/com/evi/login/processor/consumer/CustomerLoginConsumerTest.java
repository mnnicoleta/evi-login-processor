package com.evi.login.processor.consumer;

import com.evi.login.processor.mapper.LoginTrackingResultMapper;
import com.evi.login.processor.model.CustomerLoginEvent;
import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.model.RequestResult;
import com.evi.login.processor.service.LoginProcessingService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.support.Acknowledgment;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.*;

class CustomerLoginConsumerTest {

    private LoginProcessingService service;
    private CustomerLoginConsumer consumer;
    private LoginTrackingResultMapper mapper;

    @BeforeEach
    void setup() {
        mapper = Mockito.mock(LoginTrackingResultMapper.class);
        service = mock(LoginProcessingService.class);
        Acknowledgment ack = mock(Acknowledgment.class);
        consumer = new CustomerLoginConsumer(service, mapper);

    }

    @Test
    void consume_callsServiceWithMappedEvent() {
        // Prepare test data
        CustomerLoginEvent event = new CustomerLoginEvent(
                UUID.randomUUID(),
                "user1",
                "web",
                Instant.now(),
                UUID.randomUUID(),
                "127.0.0.1"
        );

        Map<String, Object> headers = new HashMap<>();

        LoginTrackingResultEvent mappedResult = new LoginTrackingResultEvent(
                event.getCustomerId(),
                event.getUsername(),
                event.getClient(),
                Instant.now(),
                UUID.randomUUID(),
                event.getCustomerIp(),
                RequestResult.SUCCESSFUL
        );

        // Mock mapper and service
        when(mapper.toResult(event)).thenReturn(mappedResult);
        when(service.processLogin(mappedResult, headers)).thenReturn(Mono.empty());

        // Call the consumer
        consumer.consume(event, headers);

        // Verify service is called with mapped event
        verify(service, times(1)).processLogin(mappedResult, headers);
    }
}
