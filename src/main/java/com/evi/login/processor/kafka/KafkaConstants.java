package com.evi.login.processor.kafka;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class KafkaConstants {

    // Topics
    public static final String CUSTOMER_LOGIN = "customer-login";

    public static final String CUSTOMER_LOGIN_RESULT = "customer-login-result";
    public static final String LOGIN_TRACKING_RESULT = "login-tracking-result";

    // Producers + CUSTOMER_LOGIN
    public static final String LOGIN_TRACKING_RESULT_PRODUCER = "login-tracking-result-producer";
    public static final String LOGIN_TRACKING_RESULT_ENTITY_PRODUCER = "login-tracking-result-entity-producer";

    // Consumers
    public static final String CONSUMER_CUSTOMER_LOGIN = "consumer-customer-login";
    public static final String CONSUMER_CUSTOMER_LOGIN_RESULT = "consumer-customer-login-result";

}
