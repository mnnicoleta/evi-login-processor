package com.evi.login.processor.service;

import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.model.RequestResult;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class LoginProcessingServiceTest {

    public static final String BASIC_AUTHENTICATION = "Basic authentication";
    public static final String TRACK_LOGING_CUSTOMER_ID_PATH = "/trackLoging/{customerId}";
    private WebClient webClient;
    private KafkaSender<String, LoginTrackingResultEvent> sender;
    private LoginProcessingService service;

    @BeforeEach
    void setup() {
        webClient = mock(WebClient.class);
        sender = mock(KafkaSender.class);
        service = new LoginProcessingService(webClient, sender);
    }

    private LoginTrackingResultEvent createEvent() {
        return new LoginTrackingResultEvent(
                UUID.randomUUID(),
                "user1",
                "web",
                Instant.now(),
                UUID.randomUUID(),
                "127.0.0.1",
                RequestResult.SUCCESSFUL
        );
    }

    @Test
    void processLoginWithoutAuthorizationReturnsUnsuccessful() {
        LoginTrackingResultEvent event = createEvent();
        Headers headers = mock(Headers.class); // no Authorization

        // Mock KafkaSender.send() to return empty Flux
        when(sender.send(any())).thenReturn(Flux.empty());

        Mono<LoginTrackingResultEvent> resultMono = service.processLogin(event, headers);

        StepVerifier.create(resultMono)
                .assertNext(result -> assertEquals(RequestResult.UNSUCCESSFUL, result.requestResult()))
                .verifyComplete();

        // Verify the KafkaSender was called with the correct event
        @SuppressWarnings("unchecked")
        ArgumentCaptor<Mono<SenderRecord<String, LoginTrackingResultEvent, String>>> captor =
                ArgumentCaptor.forClass(Mono.class);
        verify(sender).send(captor.capture());

        SenderRecord<String, LoginTrackingResultEvent, String> sentRecord = captor.getValue().block();
        assertEquals(RequestResult.UNSUCCESSFUL, sentRecord.value().requestResult());
        assertEquals(event.customerId(), sentRecord.value().customerId());
    }

    @Test
    void processLoginWithAuthorizationSuccessfulWebClientCall() {
        LoginTrackingResultEvent event = createEvent();
        Headers headers = new RecordHeaders()
                .add(new RecordHeader("Authorization", BASIC_AUTHENTICATION.getBytes()));

        // Mock WebClient
        WebClient.RequestBodyUriSpec uriSpec = mock(WebClient.RequestBodyUriSpec.class);
        WebClient.RequestBodySpec bodySpec = mock(WebClient.RequestBodySpec.class);
        WebClient.ResponseSpec responseSpec = mock(WebClient.ResponseSpec.class);

        when(webClient.post()).thenReturn(uriSpec);
        when(uriSpec.uri(TRACK_LOGING_CUSTOMER_ID_PATH, event.customerId())).thenReturn(bodySpec);
        when(bodySpec.header(HttpHeaders.AUTHORIZATION, BASIC_AUTHENTICATION)).thenReturn(bodySpec);
        when(bodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.toBodilessEntity()).thenReturn(Mono.just(ResponseEntity.ok().build()));

        when(sender.send(any())).thenReturn(Flux.empty());

        Mono<LoginTrackingResultEvent> resultMono = service.processLogin(event, headers);

        StepVerifier.create(resultMono)
                .assertNext(result -> assertEquals(RequestResult.SUCCESSFUL, result.requestResult()))
                .verifyComplete();

        verify(sender).send(any());
    }

    @Test
    void processLoginWebClientErrorReturnsUnsuccessful() {
        LoginTrackingResultEvent event = createEvent();
        Headers headers = new RecordHeaders()
                .add(new RecordHeader("Authorization", BASIC_AUTHENTICATION.getBytes()));

        // Mock WebClient
        WebClient.RequestBodyUriSpec uriSpec = mock(WebClient.RequestBodyUriSpec.class);
        WebClient.RequestBodySpec bodySpec = mock(WebClient.RequestBodySpec.class);
        WebClient.ResponseSpec responseSpec = mock(WebClient.ResponseSpec.class);

        when(webClient.post()).thenReturn(uriSpec);
        when(uriSpec.uri(TRACK_LOGING_CUSTOMER_ID_PATH, event.customerId())).thenReturn(bodySpec);
        when(bodySpec.header(HttpHeaders.AUTHORIZATION, BASIC_AUTHENTICATION)).thenReturn(bodySpec);
        when(bodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.toBodilessEntity()).thenReturn(Mono.error(new RuntimeException("fail")));

        when(sender.send(any())).thenReturn(Flux.empty());

        Mono<LoginTrackingResultEvent> resultMono = service.processLogin(event, headers);

        StepVerifier.create(resultMono)
                .assertNext(result -> assertEquals(RequestResult.UNSUCCESSFUL, result.requestResult()))
                .verifyComplete();

        verify(sender).send(any());
    }
}