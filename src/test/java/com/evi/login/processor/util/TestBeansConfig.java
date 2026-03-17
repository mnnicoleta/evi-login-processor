package com.evi.login.processor.util;

import com.evi.login.processor.consumer.CustomerLoginConsumer;
import com.evi.login.processor.consumer.CustomerLoginResultConsumer;
import com.evi.login.processor.repository.LoginTrackingRepository;
import com.evi.login.processor.service.LoginProcessingService;
import org.mockito.Mockito;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.kafka.sender.KafkaSender;

/**
 * Mocked beans
 */
@TestConfiguration
public class TestBeansConfig {

    @Bean
    @Primary
    public WebClient webClient() {
        return Mockito.mock(WebClient.class);
    }

    @Bean
    @Primary
    public KafkaSender<String, Object> producer() {
        return Mockito.mock(KafkaSender.class);
    }

    @Bean
    @Primary
    public LoginProcessingService loginProcessingService() {
        return Mockito.mock(LoginProcessingService.class);
    }

    @Bean
    @Primary
    public LoginTrackingRepository reactiveRepository() {
        return Mockito.mock(LoginTrackingRepository.class);
    }

    @Bean
    @Primary
    public CustomerLoginConsumer customerLoginConsumer() {
        return Mockito.mock(CustomerLoginConsumer.class);
    }

    @Bean
    @Primary
    public CustomerLoginResultConsumer customerLoginResultConsumer() {
        return Mockito.mock(CustomerLoginResultConsumer.class);
    }
}