package com.evi.login.data.processor.service;

import com.evi.login.data.processor.model.LoginTrackingResultEvent;
import com.evi.login.data.processor.model.RequestResult;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

import static com.evi.login.data.processor.constants.KafkaConstants.LOGIN_TRACKING_RESULT;

@Service
public class LoginProcessingService {

    private final WebClient webClient;
    private final ReactiveKafkaProducerTemplate<String, LoginTrackingResultEvent> producer;

    public LoginProcessingService(WebClient webClient,
                                  ReactiveKafkaProducerTemplate<String, LoginTrackingResultEvent> producer) {
        this.webClient = webClient;
        this.producer = producer;
    }

    public Mono<LoginTrackingResultEvent> processLogin(LoginTrackingResultEvent event) {

        return webClient.get()
                .uri("/trackLoging/{customerId}", event.customerId())
                .retrieve()
                .toBodilessEntity()
                .map(response ->
                        new LoginTrackingResultEvent(event.customerId(), event.username(), event.client(), event.timestamp(),
                                event.messageId(), event.customerIp(), RequestResult.SUCCESSFUL.getValue()))
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                        .filter(ex -> true))
                .onErrorResume(ex ->
                        Mono.just(new LoginTrackingResultEvent(event.customerId(), event.username(), event.client(),
                                event.timestamp(), event.messageId(), event.customerIp(), RequestResult.UNSUCCESSFUL.getValue())))
                .flatMap(result -> producer.send(LOGIN_TRACKING_RESULT, event.messageId().toString(), result))
                .thenReturn(event);
    }
}