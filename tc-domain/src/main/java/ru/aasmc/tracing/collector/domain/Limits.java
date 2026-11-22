package ru.aasmc.tracing.collector.domain;

import java.time.Duration;

/**
 * Checks that a configured limit, such as a maximum length or a time window, is positive. A broken limit is a mistake
 * in the configuration, not a broken business rule, so it is reported as a plain {@link IllegalArgumentException}
 * naming the setting.
 */
public final class Limits {

    private Limits() {}

    public static void requirePositive(String name, long value) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive, not " + value);
        }
    }

    public static void requirePositive(String name, Duration value) {
        if (value == null || !value.isPositive()) {
            throw new IllegalArgumentException(name + " must be positive, not " + value);
        }
    }
}
