package com.evi.login.processor.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

import static com.evi.login.processor.constants.KafkaConstants.*;

@Profile({"local"})
//@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic customerLoginTopic() {
        // 3 partitions, replication factor 3 (depending on how much durability you want)
        return new NewTopic(CUSTOMER_LOGIN, 3, (short) 3);
    }

    // Topic: customer-login-result
    @Bean
    public NewTopic customerLoginResultTopic() {
        return new NewTopic(CUSTOMER_LOGIN_RESULT, 3, (short) 3);
    }

    // Topic: login-tracking-result
    @Bean
    public NewTopic loginTrackingResultTopic() {
        return new NewTopic(LOGIN_TRACKING_RESULT, 3, (short) 3);
    }
}
