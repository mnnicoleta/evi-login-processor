package com.evi.login.processor.service;

import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.model.RequestResult;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClient.RequestHeadersSpec;
import org.springframework.web.reactive.function.client.WebClient.RequestHeadersUriSpec;
import org.springframework.web.reactive.function.client.WebClient.ResponseSpec;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.UUID;

import static com.evi.login.processor.constants.KafkaConstants.LOGIN_TRACKING_RESULT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

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
    void processLogin_withoutAuthorization_returnsUnsuccessful() {
        LoginTrackingResultEvent event = createEvent();
        RecordHeaders headers = new RecordHeaders(); // no Authorization header

        when(producer.send(anyString(), anyString(), any())).thenReturn(Mono.empty());

        Mono<LoginTrackingResultEvent> resultMono = service.processLogin(event, headers);

        // Subscribe with StepVerifier to ensure producer.send() executes
        StepVerifier.create(resultMono)
                .assertNext(result -> assertEquals(RequestResult.UNSUCCESSFUL, result.requestResult()))
                .verifyComplete();

        // Verify producer sent unsuccessful result
        ArgumentCaptor<LoginTrackingResultEvent> captor = ArgumentCaptor.forClass(LoginTrackingResultEvent.class);
        verify(producer).send(eq(LOGIN_TRACKING_RESULT), eq(event.messageId().toString()), captor.capture());
        assertEquals(RequestResult.UNSUCCESSFUL, captor.getValue().requestResult());
    }

    @Test
    void processLogin_withAuthorization_successfulWebClientCall() {
        LoginTrackingResultEvent event = createEvent();
        RecordHeaders headers = new RecordHeaders();
        headers.add("Authorization", "Basic dGVzdDp0ZXN0".getBytes());

        // Mock WebClient fluent chain
        @SuppressWarnings({"rawtypes", "unchecked"})
        RequestHeadersUriSpec uriSpec = mock(RequestHeadersUriSpec.class);
        @SuppressWarnings({"rawtypes", "unchecked"})
        RequestHeadersSpec headersSpec = mock(RequestHeadersSpec.class);
        ResponseSpec responseSpec = mock(ResponseSpec.class);

        when(webClient.get()).thenReturn(uriSpec);
        when(uriSpec.uri("/trackLoging/{customerId}", event.customerId())).thenReturn(headersSpec);
        when(headersSpec.header(HttpHeaders.AUTHORIZATION, "Basic dGVzdDp0ZXN0")).thenReturn(headersSpec);
        when(headersSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.toBodilessEntity()).thenReturn(Mono.just(ResponseEntity.ok().build())); // simulate success

        when(producer.send(anyString(), anyString(), any())).thenReturn(Mono.empty());

        Mono<LoginTrackingResultEvent> resultMono = service.processLogin(event, headers);

        // StepVerifier subscribes to the full reactive chain
        StepVerifier.create(resultMono)
                .assertNext(result -> assertEquals(RequestResult.SUCCESSFUL, result.requestResult()))
                .verifyComplete();

        verify(producer).send(eq(LOGIN_TRACKING_RESULT), eq(event.messageId().toString()), any());
    }

    @Test
    void processLogin_webClientError_returnsUnsuccessful() {
        LoginTrackingResultEvent event = createEvent();
        RecordHeaders headers = new RecordHeaders();
        headers.add("Authorization", "Basic dGVzdDp0ZXN0".getBytes());

        // Mock WebClient fluent chain with error
        @SuppressWarnings({"rawtypes", "unchecked"})
        RequestHeadersUriSpec uriSpec = mock(RequestHeadersUriSpec.class);
        @SuppressWarnings({"rawtypes", "unchecked"})
        RequestHeadersSpec headersSpec = mock(RequestHeadersSpec.class);
        ResponseSpec responseSpec = mock(ResponseSpec.class);

        when(webClient.get()).thenReturn(uriSpec);
        when(uriSpec.uri("/trackLoging/{customerId}", event.customerId())).thenReturn(headersSpec);
        when(headersSpec.header(HttpHeaders.AUTHORIZATION, "Basic dGVzdDp0ZXN0")).thenReturn(headersSpec);
        when(headersSpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.toBodilessEntity()).thenReturn(Mono.error(new RuntimeException("fail")));

        when(producer.send(anyString(), anyString(), any())).thenReturn(Mono.empty());

        Mono<LoginTrackingResultEvent> resultMono = service.processLogin(event, headers);

        StepVerifier.create(resultMono)
                .assertNext(result -> assertEquals(RequestResult.UNSUCCESSFUL, result.requestResult()))
                .verifyComplete();

        verify(producer).send(eq(LOGIN_TRACKING_RESULT), eq(event.messageId().toString()), any());
    }
}