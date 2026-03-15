package com.evi.login.processor.service;

import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.model.RequestResult;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.http.HttpHeaders;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static com.evi.login.processor.constants.KafkaConstants.LOGIN_TRACKING_RESULT;

@Service
public class LoginProcessingService {

    private final WebClient webClient;
    private final ReactiveKafkaProducerTemplate<String, LoginTrackingResultEvent> producer;

    public LoginProcessingService(WebClient webClient,
                                  ReactiveKafkaProducerTemplate<String, LoginTrackingResultEvent> producer) {
        this.webClient = webClient;
        this.producer = producer;
    }

    private static String extractAuthorization(Headers headers) {
        Header authHeader = headers.lastHeader("Authorization");
        if (authHeader != null) {
            return new String(authHeader.value(), StandardCharsets.UTF_8);
        } else {
            return null;
        }

    }

    public Mono<LoginTrackingResultEvent> processLogin(LoginTrackingResultEvent event, Headers headers) {
        String authorization = extractAuthorization(headers);

        // If authorization is missing, immediately return unsuccessful result
        if (authorization == null) {
            return ifNoAuthReturnUnsuccessfull(event);
        } else {
            return webClient.get()
                    .uri("/trackLoging/{customerId}", event.customerId())
                    .header(HttpHeaders.AUTHORIZATION, authorization)
                    .retrieve()
                    .toBodilessEntity()
                    .map(response ->
                            new LoginTrackingResultEvent(
                                    event.customerId(),
                                    event.username(),
                                    event.client(),
                                    event.timestamp(),
                                    event.messageId(),
                                    event.customerIp(),
                                    RequestResult.SUCCESSFUL))
                    .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                            .filter(ex -> true))
                    .onErrorResume(ex ->
                            Mono.just(new LoginTrackingResultEvent(
                                    event.customerId(),
                                    event.username(),
                                    event.client(),
                                    event.timestamp(),
                                    event.messageId(),
                                    event.customerIp(),
                                    RequestResult.UNSUCCESSFUL)))
                    .flatMap(result ->
                            producer.send(LOGIN_TRACKING_RESULT, event.messageId().toString(), result)
                                    .thenReturn(result)); // correct propagation result
        }
    }

    private Mono<LoginTrackingResultEvent> ifNoAuthReturnUnsuccessfull(LoginTrackingResultEvent event) {

        LoginTrackingResultEvent unsuccessfulResult = new LoginTrackingResultEvent(
                event.customerId(),
                event.username(),
                event.client(),
                event.timestamp(),
                event.messageId(),
                event.customerIp(),
                RequestResult.UNSUCCESSFUL);

        // send to Kafka even if unsuccessful
        return producer.send(LOGIN_TRACKING_RESULT, event.messageId().toString(), unsuccessfulResult)
                .thenReturn(unsuccessfulResult);
    }

}
