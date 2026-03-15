package com.evi.login.processor.mapper;

import com.evi.login.processor.model.LoginTrackingResultEvent;
import com.evi.login.processor.model.CustomerLoginEvent;
import com.evi.login.processor.entity.LoginTrackingResultEntity;
import com.evi.login.processor.model.RequestResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;


class LoginTrackingResultMapperTest {

    private LoginTrackingResultMapper mapper;

    @BeforeEach
    void setUp() {
        // MapStruct generates the implementation at compile-time
        mapper = Mappers.getMapper(LoginTrackingResultMapper.class);
    }

    @Test
    void toResult_shouldMapCustomerLoginEventToLoginTrackingResultEvent() {
        // Given: record DTO
        CustomerLoginEvent customerEvent = new CustomerLoginEvent(
                UUID.randomUUID(),
                "alice",
                "web",
                Instant.now(),
                UUID.randomUUID(),
                "msg-001"

        );

        // When: mapped
        LoginTrackingResultEvent result = mapper.toResult(customerEvent);

        // Then: all record components mapped correctly
        assertThat(result.customerId()).isEqualTo(customerEvent.customerId());
        assertThat(result.username()).isEqualTo(customerEvent.username());
        assertThat(result.client()).isEqualTo(customerEvent.client());
        assertThat(result.timestamp()).isEqualTo(customerEvent.timestamp());
        assertThat(result.customerIp()).isEqualTo(customerEvent.customerIp());
        assertThat(result.messageId()).isEqualTo(customerEvent.messageId());

        // Expression mapping
        assertThat(result.requestResult()).isEqualTo(RequestResult.UNSUCCESSFUL);
    }

    @Test
    void toEntity_shouldMapLoginTrackingResultEventToEntityIgnoringLoginResultId() {
        // Given: record DTO
        LoginTrackingResultEvent resultEvent = new LoginTrackingResultEvent(
                UUID.randomUUID(),
                "alice",
                "web",
                Instant.now(),
                UUID.randomUUID(),
                "msg-001",
                RequestResult.SUCCESSFUL
        );

        // When: mapped to entity
        LoginTrackingResultEntity entity = mapper.toEntity(resultEvent);

        // Then: fields copied correctly
        assertThat(entity.getCustomerId()).isEqualTo(resultEvent.customerId());
        assertThat(entity.getUsername()).isEqualTo(resultEvent.username());
        assertThat(entity.getClient()).isEqualTo(resultEvent.client());
        assertThat(entity.getTimestamp()).isEqualTo(resultEvent.timestamp());
        assertThat(entity.getMessageId()).isEqualTo(resultEvent.messageId());
        assertThat(entity.getCustomerIp()).isEqualTo(resultEvent.customerIp());
        assertThat(entity.getRequestResult()).isEqualTo(resultEvent.requestResult());

        // Ignored DB-generated field should remain null
        assertThat(entity.getLoginResultId()).isNull();
    }

    @Test
    void toResult_shouldReturnNullWhenInputIsNull() {
        // When
        LoginTrackingResultEvent result = mapper.toResult(null);

        // Then
        assertThat(result).isNull();
    }

    @Test
    void toEntity_shouldReturnNullWhenInputIsNull() {
        // When
        LoginTrackingResultEntity entity = mapper.toEntity(null);

        // Then
        assertThat(entity).isNull();
    }
}