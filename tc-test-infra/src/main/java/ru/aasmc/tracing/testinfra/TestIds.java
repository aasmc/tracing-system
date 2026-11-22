package ru.aasmc.tracing.testinfra;

import java.util.HexFormat;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unique identifiers per test (09 §0.3 T8: isolation through unique data, not cleanup). Names carry a JVM-wide
 * sequence number plus a random suffix, so they are unique across tests, forks and reused containers.
 */
public final class TestIds {

    private static final HexFormat HEX = HexFormat.of();
    private static final AtomicLong SEQUENCE = new AtomicLong();

    private TestIds() {}

    /** 32 lowercase hex characters, never all zeros (W3C trace id). */
    public static String traceId() {
        return nonZeroHex(16);
    }

    /** 16 lowercase hex characters, never all zeros (W3C span id). */
    public static String spanId() {
        return nonZeroHex(8);
    }

    /** {@code prefix-<seq>-<random>}, usable as a source name, correlation key value, consumer group, etc. */
    public static String name(String prefix) {
        return prefix + "-" + SEQUENCE.incrementAndGet() + "-"
                + HEX.toHexDigits(ThreadLocalRandom.current().nextInt());
    }

    /** A valid Kafka topic name ({@code [a-zA-Z0-9._-]}, at most 249 characters). */
    public static String topic(String prefix) {
        return name(prefix).replaceAll("[^a-zA-Z0-9._-]", "_");
    }

    private static String nonZeroHex(int bytes) {
        byte[] value = new byte[bytes];
        do {
            ThreadLocalRandom.current().nextBytes(value);
        } while (isAllZero(value));
        return HEX.formatHex(value);
    }

    private static boolean isAllZero(byte[] value) {
        for (byte b : value) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }
}
