package com.evi.login.processor.consumer;

import com.evi.login.processor.entity.LoginTrackingResultEntity;
import com.evi.login.processor.mapper.LoginTrackingResultMapper;
import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.repository.LoginTrackingRepository;
import com.evi.login.processor.transaction.ReactiveTransactionExecutor;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import static com.evi.login.processor.config.kafka.KafkaConstants.LOGIN_TRACKING_RESULT;

/**
 * Fully reactive consumer: consumes LoginTrackingResultEvent, saves into DB, publishes result to next topic.
 */
@Slf4j
@Component
public class CustomerLoginResultConsumer {

    private final LoginTrackingRepository repository;
    private final KafkaSender<String, LoginTrackingResultEntity> loginTrackingResultEntitySender;
    private final LoginTrackingResultMapper mapper;
    private final KafkaReceiver<String, LoginTrackingResultEvent> kafkaReceiver;
    private final ReactiveTransactionExecutor transactionExecutor;

    @Getter
    private Disposable subscription;

    public CustomerLoginResultConsumer(LoginTrackingRepository repository,
                                       KafkaSender<String, LoginTrackingResultEntity> loginTrackingResultEntitySender,
                                       LoginTrackingResultMapper mapper,
                                       KafkaReceiver<String, LoginTrackingResultEvent> kafkaReceiver,
                                       ReactiveTransactionExecutor transactionExecutor) {
        this.repository = repository;
        this.loginTrackingResultEntitySender = loginTrackingResultEntitySender;
        this.mapper = mapper;
        this.kafkaReceiver = kafkaReceiver;
        this.transactionExecutor = transactionExecutor;
    }

    // Called by Spring after all dependencies are injected
    @PostConstruct
    public void init() {
        startConsuming();
    }

    private void startConsuming() {
        this.subscription = kafkaReceiver
                .receive()
                .flatMap(receivedRecord -> processEvent(receivedRecord.value())
                        .then(Mono.fromRunnable(receivedRecord.receiverOffset()::acknowledge))
                        .retry(3) // retry failed events
                )
                .subscribe(
                        null,
                        err -> log.error("Reactive Kafka stream failed", err),
                        () -> log.info("Reactive Kafka stream completed")
                );
    }

    /**
     * Public method to process a single LoginTrackingResultEvent.
     * This is directly testable in unit tests without Kafka.
     */
    public Mono<Void> processEvent(LoginTrackingResultEvent event) {
        return transactionExecutor.execute(
                repository.save(mapper.toEntity(event))
                        .flatMap(entity -> {
                            SenderRecord<String, LoginTrackingResultEntity, String> senderRecord =
                                    SenderRecord.create(
                                            new ProducerRecord<>(LOGIN_TRACKING_RESULT, entity.getMessageId().toString(), entity),
                                            entity.getMessageId().toString()
                                    );

                            return loginTrackingResultEntitySender.send(Mono.just(senderRecord))
                                    .then();
                        })
                        .doOnSuccess(v -> log.debug("Processed and saved: {}", event))
                        .doOnError(err -> log.error("Failed to process event: {}", event, err))
                        .then());
    }
}