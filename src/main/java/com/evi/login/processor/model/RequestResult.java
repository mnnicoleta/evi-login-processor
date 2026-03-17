package com.evi.login.processor.model;

import lombok.Getter;

/**
 * Result of REST call
 */
@Getter
public enum RequestResult {

    SUCCESSFUL("successful"),
    UNSUCCESSFUL("unsuccessful");

    private final String value;

    RequestResult(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
