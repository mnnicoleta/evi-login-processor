package com.evi.login.processor.service;

import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.model.RequestResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.evi.login.processor.kafka.KafkaConstants.CUSTOMER_LOGIN_RESULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

/**
 * LoginProcessingServiceTest
 */
class LoginProcessingServiceTest {

    private WebClient webClient;
    private ReactiveKafkaProducerTemplate<String, LoginTrackingResultEvent> producer;
    private LoginProcessingService service;

    @BeforeEach
    void setup() {
        webClient = mock(WebClient.class);
        producer = mock(ReactiveKafkaProducerTemplate.class);
        service = new LoginProcessingService(webClient, producer);
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
    void processLoginWithoutAuthorization_returnsUnsuccessful() {
        LoginTrackingResultEvent event = createEvent();
        Map<String, Object> headers = new HashMap<>(); // no Authorization header

        when(producer.send(anyString(), anyString(), any())).thenReturn(Mono.empty());

        Mono<? extends LoginTrackingResultEvent> resultMono = service.processLogin(event, headers);

        // Subscribe with StepVerifier to ensure producer.send() executes
        StepVerifier.create(resultMono)
                .assertNext(result ->
                        assertEquals(RequestResult.UNSUCCESSFUL, result.getRequestResult()))
                .verifyComplete();

        // Verify producer sent unsuccessful result
        ArgumentCaptor<LoginTrackingResultEvent> captor = ArgumentCaptor.forClass(LoginTrackingResultEvent.class);
        verify(producer).send(eq(CUSTOMER_LOGIN_RESULT), eq(event.getCustomerId().toString()), captor.capture());
        assertEquals(RequestResult.UNSUCCESSFUL, captor.getValue().getRequestResult());
    }

    @Test
    void processLoginEventWithAuthorizationSuccessfulWebClientCall() {
        LoginTrackingResultEvent event = createEvent();
        Map<String, Object> headers = new HashMap<>();
        headers.put("Authorization", "Basic dGVzdDp0ZXN0".getBytes());

        // Mock WebClient chain properly
        WebClient.RequestBodySpec requestBodySpec = mock(WebClient.RequestBodySpec.class);
        WebClient.RequestBodyUriSpec uriSpec = mock(WebClient.RequestBodyUriSpec.class);
        WebClient.ResponseSpec responseSpec = mock(WebClient.ResponseSpec.class);

        when(webClient.post()).thenReturn(uriSpec);
        when(uriSpec.uri("/trackLoging/{customerId}", event.getCustomerId())).thenReturn(requestBodySpec);
        when(requestBodySpec.header(HttpHeaders.AUTHORIZATION, "Basic dGVzdDp0ZXN0")).thenReturn(requestBodySpec);

        when(requestBodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.toBodilessEntity()).thenReturn(Mono.just(ResponseEntity.ok().build())); // simulate success

        // Mock producer
        when(producer.send(anyString(), anyString(), any())).thenReturn(Mono.empty());

        // Act
        Mono<? extends LoginTrackingResultEvent> resultMono = service.processLogin(event, headers);

        // Assert
        StepVerifier.create(resultMono)
                .assertNext(result -> assertEquals(RequestResult.SUCCESSFUL, result.getRequestResult()))
                .verifyComplete();

        verify(producer).send(eq(CUSTOMER_LOGIN_RESULT), eq(event.getCustomerId().toString()), any());
    }

    @Test
    void processLoginWebClientErrorReturnsUnsuccessful() {
        LoginTrackingResultEvent event = createEvent();
        Map<String, Object> headers = new HashMap<>();
        headers.put("Authorization", "Basic dGVzdDp0ZXN0".getBytes());

        // Mock WebClient fluent chain with error
        WebClient.RequestBodySpec requestBodySpec = mock(WebClient.RequestBodySpec.class);
        WebClient.RequestBodyUriSpec uriSpec = mock(WebClient.RequestBodyUriSpec.class);
        WebClient.ResponseSpec responseSpec = mock(WebClient.ResponseSpec.class);

        when(webClient.post()).thenReturn(uriSpec);
        when(uriSpec.uri("/trackLoging/{customerId}", event.getCustomerId())).thenReturn(requestBodySpec);
        when(requestBodySpec.header(HttpHeaders.AUTHORIZATION, "Basic dGVzdDp0ZXN0")).thenReturn(requestBodySpec);

        when(requestBodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.toBodilessEntity()).thenReturn(Mono.error(new RuntimeException("fail")));

        when(producer.send(anyString(), anyString(), any())).thenReturn(Mono.empty());

        Mono<? extends LoginTrackingResultEvent> resultMono = service.processLogin(event, headers);

        StepVerifier.create(resultMono)
                .assertNext(result -> assertEquals(RequestResult.UNSUCCESSFUL, result.getRequestResult()))
                .verifyComplete();

        verify(producer).send(eq(CUSTOMER_LOGIN_RESULT), eq(event.getCustomerId().toString()), any());
    }
}