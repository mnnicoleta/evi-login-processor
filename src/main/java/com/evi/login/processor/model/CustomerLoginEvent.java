package com.evi.login.processor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jdk.jfr.Description;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Description("from topic customer-login")

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomerLoginEvent {

    private UUID customerId;
    private String username;
    private String client;
    private Instant timestamp;
    private UUID messageId;
    private String customerIp;
}