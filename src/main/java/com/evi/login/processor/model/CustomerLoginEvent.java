package com.evi.login.processor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jdk.jfr.Description;

import java.time.Instant;
import java.util.UUID;

@Description("from topic customer-login")
@JsonIgnoreProperties(ignoreUnknown = true)
public record CustomerLoginEvent(
        UUID customerId,
        String username,
        String client,
        Instant timestamp,
        UUID messageId,
        String customerIp) {
}
