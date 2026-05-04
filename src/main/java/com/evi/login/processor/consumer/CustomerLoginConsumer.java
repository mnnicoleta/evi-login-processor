package com.evi.login.processor.consumer;

import com.evi.login.processor.mapper.LoginTrackingResultMapper;
import com.evi.login.processor.model.CustomerLoginEvent;
import com.evi.login.processor.service.LoginProcessingService;
import com.evi.login.processor.transaction.ReactiveTransactionExecutor;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;

/**
 * Fully reactive consumer for CUSTOMER_LOGIN topic.
 * Reads CustomerLoginEvent from Kafka, executes REST call,
 * and publishes results.
 */
@Slf4j
@Component
public class CustomerLoginConsumer {

    private final LoginProcessingService service;
    private final LoginTrackingResultMapper mapper;
    private final KafkaReceiver<String, CustomerLoginEvent> kafkaReceiverCustomerLoginEvent;
    private final ReactiveTransactionExecutor transactionExecutor;
    @Getter
    private Disposable subscription;

    public CustomerLoginConsumer(LoginProcessingService service,
                                 LoginTrackingResultMapper mapper,
                                 KafkaReceiver<String, CustomerLoginEvent> kafkaReceiverCustomerLoginEvent,
                                 ReactiveTransactionExecutor transactionExecutor) {
        this.service = service;
        this.mapper = mapper;
        this.kafkaReceiverCustomerLoginEvent = kafkaReceiverCustomerLoginEvent;
        this.transactionExecutor = transactionExecutor;
    }

    @PostConstruct
    public void init() {
        startConsuming();
    }

    private void startConsuming() {
        this.subscription = kafkaReceiverCustomerLoginEvent
                .receive()
                .flatMap(receivedRecord -> processEvent(receivedRecord.value(), receivedRecord.headers())
                        .then(Mono.fromRunnable(receivedRecord.receiverOffset()::acknowledge))
                        .retry(3)
                )
                .subscribe(
                        null,
                        err -> log.error("Reactive stream error", err),
                        () -> log.info("KafkaReceiver stream completed")
                );
    }

    /**
     * Public method to process a single event (for unit testing)
     */
    public Mono<Void> processEvent(CustomerLoginEvent event, org.apache.kafka.common.header.Headers headers) {
        return transactionExecutor.execute(
                service.processLogin(mapper.toResult(event), headers)
                        .doOnSuccess(r -> log.info("Processed CustomerLoginEvent: {}", event))
                        .doOnError(err -> log.error("Failed processing: {}", event, err))
                        .then());
    }
}