package com.evi.login.processor.mapper;

import com.evi.login.processor.entity.LoginTrackingResultEntity;
import com.evi.login.processor.model.CustomerLoginEvent;
import com.evi.login.processor.model.LoginTrackingResultEvent;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@SuppressWarnings("unused")
@Mapper(componentModel = "spring")
public interface LoginTrackingResultMapper {

    @Mapping(target = "requestResult", expression = "java(com.evi.login.processor.model.RequestResult.UNSUCCESSFUL)")
    LoginTrackingResultEvent toResult(CustomerLoginEvent customerLoginEvent);

    @Mapping(target = "loginResultId", ignore = true)
// DB generates this
    LoginTrackingResultEntity toEntity(LoginTrackingResultEvent loginTrackingResultEvent);
}
