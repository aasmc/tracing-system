package ru.aasmc.tracing.collector.domain;

import java.time.Duration;

/**
 * Checks of configured limits. A broken limit is a configuration error, not a broken business rule, so it is reported
 * as {@link IllegalArgumentException}.
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
