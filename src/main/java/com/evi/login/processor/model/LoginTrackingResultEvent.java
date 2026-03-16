package com.evi.login.processor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jdk.jfr.Description;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Description("from topic customer-login-result, to topic login-tracking-result")
@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginTrackingResultEvent {
    private UUID customerId;
    private String username;
    private String client;
    private Instant timestamp;
    private UUID messageId;
    private String customerIp;
    private RequestResult requestResult;

}