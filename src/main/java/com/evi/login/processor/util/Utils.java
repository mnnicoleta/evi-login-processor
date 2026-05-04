package com.evi.login.processor.util;

import lombok.experimental.UtilityClass;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;

@UtilityClass
public final class Utils {

    public static String extractHeader(Headers headers, String key) {
        if (headers == null) return null;
        var header = headers.lastHeader(key); // get last header with that key
        if (header != null && header.value() != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return null;
    }
}
