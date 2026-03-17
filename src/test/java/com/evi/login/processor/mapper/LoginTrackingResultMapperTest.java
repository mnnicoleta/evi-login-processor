package com.evi.login.processor.mapper;

import com.evi.login.processor.entity.LoginTrackingResultEntity;
import com.evi.login.processor.model.CustomerLoginEvent;
import com.evi.login.processor.model.LoginTrackingResultEvent;
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
    void toResultShouldMapCustomerLoginEventToLoginTrackingResultEvent() {
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
        assertThat(result.getCustomerId()).isEqualTo(customerEvent.getCustomerId());
        assertThat(result.getUsername()).isEqualTo(customerEvent.getUsername());
        assertThat(result.getClient()).isEqualTo(customerEvent.getClient());
        assertThat(result.getTimestamp()).isEqualTo(customerEvent.getTimestamp());
        assertThat(result.getCustomerIp()).isEqualTo(customerEvent.getCustomerIp());
        assertThat(result.getMessageId()).isEqualTo(customerEvent.getMessageId());

        // Expression mapping
        assertThat(result.getRequestResult()).isEqualTo(RequestResult.UNSUCCESSFUL);
    }

    @Test
    void toEntityShouldMapLoginTrackingResultEventToEntityIgnoringLoginResultId() {
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
        assertThat(entity.getCustomerId()).isEqualTo(resultEvent.getCustomerId());
        assertThat(entity.getUsername()).isEqualTo(resultEvent.getUsername());
        assertThat(entity.getClient()).isEqualTo(resultEvent.getClient());
        assertThat(entity.getTimestamp()).isEqualTo(resultEvent.getTimestamp());
        assertThat(entity.getMessageId()).isEqualTo(resultEvent.getMessageId());
        assertThat(entity.getCustomerIp()).isEqualTo(resultEvent.getCustomerIp());
        assertThat(entity.getRequestResult()).isEqualTo(resultEvent.getRequestResult());

        // Ignored DB-generated field should remain null
        assertThat(entity.getLoginResultId()).isNull();
    }

    @Test
    void toResultShouldReturnNullWhenInputIsNull() {
        // When
        LoginTrackingResultEvent result = mapper.toResult(null);

        // Then
        assertThat(result).isNull();
    }

    @Test
    void toEntityShouldReturnNullWhenInputIsNull() {
        // When
        LoginTrackingResultEntity entity = mapper.toEntity(null);

        // Then
        assertThat(entity).isNull();
    }
}