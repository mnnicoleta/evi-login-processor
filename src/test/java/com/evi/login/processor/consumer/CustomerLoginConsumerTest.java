package com.evi.login.processor.consumer;

import com.evi.login.processor.model.CustomerLoginEvent;
import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.model.RequestResult;
import com.evi.login.processor.mapper.LoginTrackingResultMapper;
import com.evi.login.processor.service.LoginProcessingService;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import reactor.core.publisher.Mono;

import java.time.Instant;
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

        RecordHeaders headers = new RecordHeaders();

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

        // Call the consumer
        consumer.consume(event, headers);

        // Verify service is called with mapped event
        verify(service, times(1)).processLogin(mappedResult, headers);
    }
}
