package com.evi.login.processor.util;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class UtilsTest {

    @Test
    void extractHeader_shouldReturnNull_whenHeadersIsNull() {
        assertThat(Utils.extractHeader(null, "anyKey")).isNull();
    }

    @Test
    void extractHeader_shouldReturnNull_whenHeaderDoesNotExist() {
        Headers headers = new RecordHeaders();
        assertThat(Utils.extractHeader(headers, "missingKey")).isNull();
    }

    @Test
    void extractHeader_shouldReturnHeaderValue_whenHeaderExists() {
        String key = "myKey";
        String value = "myValue";
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));

        String extracted = Utils.extractHeader(headers, key);
        assertThat(extracted).isEqualTo(value);
    }

    @Test
    void extractHeader_shouldReturnNull_whenHeaderValueIsNull() {
        String key = "nullValueKey";
        Headers headers = new RecordHeaders();
        headers.add(new RecordHeader(key, null));

        String extracted = Utils.extractHeader(headers, key);
        assertThat(extracted).isNull();
    }
}