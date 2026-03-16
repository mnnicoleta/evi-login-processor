package com.evi.login.processor.consumer;

import com.evi.login.processor.entity.LoginTrackingResultEntity;
import com.evi.login.processor.mapper.LoginTrackingResultMapper;
import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.model.RequestResult;
import com.evi.login.processor.repository.LoginTrackingRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

import static com.evi.login.processor.kafka.KafkaConstants.LOGIN_TRACKING_RESULT;
import static org.mockito.Mockito.*;

class CustomerLoginResultConsumerTest {

    private LoginTrackingRepository repository;
    private ReactiveKafkaProducerTemplate<String, LoginTrackingResultEntity> producer;
    private CustomerLoginResultConsumer consumer;
    private LoginTrackingResultMapper mapper;

    @BeforeEach
    void setup() {
        mapper = Mockito.mock(LoginTrackingResultMapper.class);
        repository = mock(LoginTrackingRepository.class);
        producer = mock(ReactiveKafkaProducerTemplate.class);

        consumer = new CustomerLoginResultConsumer(repository, producer, mapper);
    }

    @Test
    void consume_savesAndPublishesEntity() {
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
        entity.setCustomerIp("127.0.0.1");
        entity.setMessageId(messageId);
        entity.setCustomerId(customerId);

        // Mock mapper and repository behavior
        when(mapper.toEntity(event)).thenReturn(entity);
        when(repository.save(entity)).thenReturn(Mono.just(entity));
        when(producer.send(anyString(), anyString(), any())).thenReturn(Mono.empty());

        // Call the consumer
        consumer.consume(event);

        // Verify mapper, repository, and producer calls
        verify(mapper).toEntity(event);
        verify(repository).save(entity);
        verify(producer).send(LOGIN_TRACKING_RESULT, entity.getCustomerId().toString(), entity);
    }
}