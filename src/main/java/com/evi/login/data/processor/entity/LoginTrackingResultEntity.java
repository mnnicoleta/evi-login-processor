package com.evi.login.data.processor.entity;

import com.evi.login.data.processor.model.RequestResult;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jdk.jfr.Description;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

@Description("from topic customer-login-result, to topic login-tracking-result")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "login_tracking_result", schema = "login_tracker_db")
public class LoginTrackingResultEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private UUID loginResultId;
    private UUID customerId;
    private String username;
    private String client;
    private Instant timestamp;
    private UUID messageId;
    private String customerIp;
    private RequestResult requestResult;
}
