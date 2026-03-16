package com.evi.login.processor.service;

import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.model.RequestResult;
import org.springframework.http.HttpHeaders;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

import static com.evi.login.processor.kafka.KafkaConstants.CUSTOMER_LOGIN_RESULT;

@Service
public class LoginProcessingService {

    private final WebClient webClient;
    private final ReactiveKafkaProducerTemplate<String, LoginTrackingResultEvent> producer;

    public LoginProcessingService(WebClient webClient,
                                  ReactiveKafkaProducerTemplate<String, LoginTrackingResultEvent> producer) {
        this.webClient = webClient;
        this.producer = producer;
    }

    private static String extractAuthorization(Map<String, Object> headers) {
        byte[] authHeader = (byte[]) headers.get("Authorization");
        if (authHeader != null) {
            return new String(authHeader, StandardCharsets.UTF_8);
        } else {
            return null;
        }
    }

    public Mono<? extends LoginTrackingResultEvent> processLogin(LoginTrackingResultEvent event, Map<String, Object> headers) {
        String authorization = extractAuthorization(headers);

        // If authorization is missing, immediately return unsuccessful result
        if (authorization == null) {
            return ifNoAuthReturnUnsuccessfull(event);
        } else {
            return webClient.post()
                    .uri("/trackLoging/{customerId}", event.getCustomerId())
                    .header(HttpHeaders.AUTHORIZATION, authorization)
                    .retrieve()
                    .toBodilessEntity()
                    .map(response ->
                            new LoginTrackingResultEvent(
                                    event.getCustomerId(),
                                    event.getUsername(),
                                    event.getClient(),
                                    event.getTimestamp(),
                                    event.getMessageId(),
                                    event.getCustomerIp(),
                                    RequestResult.SUCCESSFUL))
                    .retryWhen(Retry.backoff(3, Duration.ofSeconds(2))
                            .filter(ex -> true))
                    .onErrorResume(ex ->
                            Mono.just(new LoginTrackingResultEvent(
                                    event.getCustomerId(),
                                    event.getUsername(),
                                    event.getClient(),
                                    event.getTimestamp(),
                                    event.getMessageId(),
                                    event.getCustomerIp(),
                                    RequestResult.UNSUCCESSFUL)))
                    .flatMap(result ->
                            producer.send(CUSTOMER_LOGIN_RESULT, event.getCustomerId().toString(), result)
                                    .thenReturn(result)); // correct propagation result
        }
    }

    private Mono<? extends LoginTrackingResultEvent> ifNoAuthReturnUnsuccessfull(LoginTrackingResultEvent event) {

        LoginTrackingResultEvent unsuccessfulResult = new LoginTrackingResultEvent(
                event.getCustomerId(),
                event.getUsername(),
                event.getClient(),
                event.getTimestamp(),
                event.getMessageId(),
                event.getCustomerIp(),
                RequestResult.UNSUCCESSFUL);

        // send to Kafka even if unsuccessful (customerId as key if order matters)
        return producer.send(CUSTOMER_LOGIN_RESULT, event.getCustomerId().toString(), unsuccessfulResult)
                .thenReturn(unsuccessfulResult);
    }

}
