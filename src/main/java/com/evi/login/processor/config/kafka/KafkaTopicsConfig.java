package com.evi.login.processor.config.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.config.TopicBuilder;

import static com.evi.login.processor.config.kafka.KafkaConstants.*;

@Profile({"local"})
@Configuration
public class KafkaTopicsConfig {

    public static final String RETENTION_MS = "retention.ms";
    public static final String DAYS_90 = "7776000000";

    @Bean
    public NewTopic customerLoginTopic() {
        return TopicBuilder.name(CUSTOMER_LOGIN)
                .partitions(1)
                .replicas(1)
                .config(RETENTION_MS, DAYS_90)  // 90 days
                .build();
    }

    // Topic: customer-login-result
    @Bean
    public NewTopic customerLoginResultTopic() {
        return TopicBuilder.name(CUSTOMER_LOGIN_RESULT)
                .partitions(1)
                .replicas(1)
                .config(RETENTION_MS, DAYS_90)  // 90 days
                .build();
    }

    // Topic: login-tracking-result
    @Bean
    public NewTopic loginTrackingResultTopic() {
        return TopicBuilder.name(LOGIN_TRACKING_RESULT)
                .partitions(1)
                .replicas(1)
                .config(RETENTION_MS, DAYS_90)  // 90 days
                .build();
    }

}
