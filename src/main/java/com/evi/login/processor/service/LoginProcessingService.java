package com.evi.login.processor.service;

import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.model.RequestResult;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.util.retry.Retry;

import java.time.Duration;

import static com.evi.login.processor.config.kafka.KafkaConstants.CUSTOMER_LOGIN_RESULT;
import static com.evi.login.processor.util.Utils.extractHeader;


/**
 * LoginProcessingService logic: read events, perform REST calls and publish results further
 */
@Service
public class LoginProcessingService {

    private final WebClient webClient;
    private final KafkaSender<String, LoginTrackingResultEvent> loginTrackingResultEventSender;

    public LoginProcessingService(WebClient webClient,
                                  KafkaSender<String, LoginTrackingResultEvent> loginTrackingResultEventSender) {
        this.webClient = webClient;
        this.loginTrackingResultEventSender = loginTrackingResultEventSender;
    }

    /**
     * Process a login event reactively, using Authorization header from Kafka headers.
     * Handles REST call, retry, error fallback, and publishes result to Kafka.
     *
     * @param event   the event to process
     * @param headers Kafka headers from the incoming record
     * @return Mono emitting processed LoginTrackingResultEvent
     */
    public Mono<LoginTrackingResultEvent> processLogin(LoginTrackingResultEvent event, Headers headers) {
        String authorization = extractHeader(headers, "Authorization");

        if (authorization == null) {
            return sendUnsuccessful(event);
        }

        return webClient.post()
                .uri("/trackLoging/{customerId}", event.customerId())
                .header(HttpHeaders.AUTHORIZATION, authorization)
                .retrieve()
                .toBodilessEntity()
                .map(response -> new LoginTrackingResultEvent(
                        event.customerId(),
                        event.username(),
                        event.client(),
                        event.timestamp(),
                        event.messageId(),
                        event.customerIp(),
                        RequestResult.SUCCESSFUL
                ))
                .retryWhen(Retry.backoff(3, Duration.ofSeconds(2)))
                .onErrorResume(ex -> Mono.just(new LoginTrackingResultEvent(
                        event.customerId(),
                        event.username(),
                        event.client(),
                        event.timestamp(),
                        event.messageId(),
                        event.customerIp(),
                        RequestResult.UNSUCCESSFUL
                )))
                .flatMap(result -> {
                    // create a Kafka record
                    ProducerRecord<String, LoginTrackingResultEvent> producerRecord =
                            new ProducerRecord<>(CUSTOMER_LOGIN_RESULT, event.messageId().toString(), result);

                    // wrap in SenderRecord for KafkaSender
                    SenderRecord<String, LoginTrackingResultEvent, String> senderRecord =
                            SenderRecord.create(producerRecord, event.messageId().toString());

                    // send reactive stream
                    return loginTrackingResultEventSender
                            .send(Mono.just(senderRecord))   // Flux<SenderResult<String>>
                            .then(Mono.just(result));        // convert to Mono<LoginTrackingResultEvent>
                });
    }


    private Mono<LoginTrackingResultEvent> sendUnsuccessful(LoginTrackingResultEvent event) {

        LoginTrackingResultEvent unsuccessfulResult = new LoginTrackingResultEvent(
                event.customerId(),
                event.username(),
                event.client(),
                event.timestamp(),
                event.messageId(),
                event.customerIp(),
                RequestResult.UNSUCCESSFUL);

        // create a Kafka record
        ProducerRecord<String, LoginTrackingResultEvent> producerRecord =
                new ProducerRecord<>(CUSTOMER_LOGIN_RESULT, event.messageId().toString(), unsuccessfulResult);

        // wrap in SenderRecord for KafkaSender
        SenderRecord<String, LoginTrackingResultEvent, String> senderRecord =
                SenderRecord.create(producerRecord, event.messageId().toString());

        // send reactive stream
        return loginTrackingResultEventSender
                .send(Mono.just(senderRecord))
                .then(Mono.just(unsuccessfulResult));
    }

}
