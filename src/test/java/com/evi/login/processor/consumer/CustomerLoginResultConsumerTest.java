package com.evi.login.processor.consumer;

import com.evi.login.processor.entity.LoginTrackingResultEntity;
import com.evi.login.processor.mapper.LoginTrackingResultMapper;
import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.model.RequestResult;
import com.evi.login.processor.repository.LoginTrackingRepository;
import com.evi.login.processor.transaction.ReactiveTransactionExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class CustomerLoginResultConsumerTest {

    private LoginTrackingRepository repository;
    private KafkaSender<String, LoginTrackingResultEntity> sender;
    private LoginTrackingResultMapper mapper;
    private KafkaReceiver<String, LoginTrackingResultEvent> receiver;
    private CustomerLoginResultConsumer consumer;
    private ReactiveTransactionExecutor transactionExecutor;

    @BeforeEach
    void setup() {
        repository = mock(LoginTrackingRepository.class);
        sender = mock(KafkaSender.class);
        mapper = mock(LoginTrackingResultMapper.class);
        receiver = mock(KafkaReceiver.class);
        transactionExecutor = mock(ReactiveTransactionExecutor.class);

        when(transactionExecutor.execute(any()))
                .thenAnswer(invocation -> {
                    return invocation.getArgument(0); // 👈 NO TRANSACTION, PURE PASS-THROUGH
                });

        when(receiver.receive()).thenReturn(Flux.empty());

        consumer = new CustomerLoginResultConsumer(repository, sender, mapper, receiver, transactionExecutor);
    }

    @Test
    void processEventSavesEntityAndSendsToKafka() {
        // Prepare test data
        UUID messageId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();

        LoginTrackingResultEvent event = new LoginTrackingResultEvent(
                customerId,
                "user1",
                "web",
                Instant.now(),
                messageId,
                "127.0.0.1",
                RequestResult.SUCCESSFUL
        );

        LoginTrackingResultEntity entity = new LoginTrackingResultEntity();
        entity.setCustomerId(customerId);
        entity.setMessageId(messageId);
        entity.setCustomerIp("127.0.0.1");

        // Mock mapper and repository
        when(mapper.toEntity(event)).thenReturn(entity);
        when(repository.save(entity)).thenReturn(Mono.just(entity));

        // Mock sender behavior
        when(sender.send(any(Mono.class))).thenReturn(Flux.empty());

        // Call the public processEvent() method
        consumer.processEvent(event).block(); // block() is okay in unit test

        // Verify interactions
        verify(mapper, times(1)).toEntity(event);
        verify(repository, times(1)).save(entity);
        verify(sender, times(1)).send(any(Mono.class));
    }
}