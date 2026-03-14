package com.evi.login.data.processor.model;

import jdk.jfr.Description;

import java.time.Instant;
import java.util.UUID;

@Description("from topic customer-login-result, to topic login-tracking-result")
public record LoginTrackingResultEvent(
        UUID customerId,
        String username,
        String client,
        Instant timestamp,
        UUID messageId,
        String customerIp,
        String requestResult // successful | unsuccessful
) {
}
