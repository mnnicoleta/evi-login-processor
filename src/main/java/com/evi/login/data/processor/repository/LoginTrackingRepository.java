package com.evi.login.data.processor.repository;

import com.evi.login.data.processor.entity.LoginTrackingResultEntity;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface LoginTrackingRepository extends R2dbcRepository<LoginTrackingResultEntity, UUID> {
    //TODO what is the impact if ReactiveCrudRepository is used?
}