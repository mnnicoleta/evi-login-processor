package com.evi.login.processor.util;

import com.github.tomakehurst.wiremock.WireMockServer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.web.reactive.function.client.WebClient;

@TestConfiguration
@Profile("local")
public class WebClientConfig {

    @Bean
    public WebClient webClient(WireMockServer wireMockServer) {
        return WebClient.builder()
                .baseUrl("http://localhost:" + wireMockServer.port())
                .build();
    }
}
