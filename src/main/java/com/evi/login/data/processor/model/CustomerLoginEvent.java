package com.evi.login.data.processor.model;

import jdk.jfr.Description;

import java.time.Instant;
import java.util.UUID;

@Description("from topic customer-login")
public record CustomerLoginEvent(
        UUID customerId,
        String username,
        String client,
        Instant timestamp,
        UUID messageId,
        String customerIp) {
}