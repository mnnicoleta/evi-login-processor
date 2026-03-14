package com.evi.login.data.processor.mapper;

import com.evi.login.data.processor.model.RequestResult; // import required
import com.evi.login.data.processor.entity.LoginTrackingResultEntity;
import com.evi.login.data.processor.model.CustomerLoginEvent;
import com.evi.login.data.processor.model.LoginTrackingResultEvent;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@SuppressWarnings("unused")
@Mapper(componentModel = "spring")
public interface LoginTrackingResultMapper {

    @Mapping(target = "requestResult", expression = "java(RequestResult.UNSUCCESSFUL.toString())")
    LoginTrackingResultEvent toResult(CustomerLoginEvent customerLoginEvent);

    @Mapping(target = "loginResultId", ignore = true)
        // DB generates this
    LoginTrackingResultEntity toEntity(LoginTrackingResultEvent loginTrackingResultEvent);
}
