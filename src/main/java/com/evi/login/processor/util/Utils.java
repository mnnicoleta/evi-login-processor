package com.evi.login.processor.util;

import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;

public final class Utils {

    private Utils() {
        /* This utility class should not be instantiated */
    }

    public static String extractHeader(Headers headers, String key) {
        if (headers == null) return null;
        var header = headers.lastHeader(key); // get last header with that key
        if (header != null && header.value() != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return null;
    }
}
