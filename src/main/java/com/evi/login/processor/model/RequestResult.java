package com.evi.login.processor.model;

public enum RequestResult {

    SUCCESSFUL("successful"),
    UNSUCCESSFUL("unsuccessful");

    private final String value;

    RequestResult(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
