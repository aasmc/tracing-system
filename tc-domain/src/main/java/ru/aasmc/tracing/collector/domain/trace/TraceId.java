package ru.aasmc.tracing.collector.domain.trace;

import java.util.UUID;

/**
 * A 128-bit W3C trace id. Its text form is 32 lowercase hex characters; in PostgreSQL it is a {@code uuid}.
 *
 * <p>Never all zeros. {@link #compareTo} orders ids as unsigned bytes, the same order PostgreSQL uses for
 * {@code uuid}, so a pagination cursor built in Java matches {@code ORDER BY trace_id}.
 *
 * @param high the most significant 64 bits
 * @param low the least significant 64 bits
 */
public record TraceId(long high, long low) implements Comparable<TraceId> {

    /**
     * Parses 32 hex characters; upper case is accepted.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_TRACE_ID} for null, a wrong
     *     length, a non-hex character or all zeros
     */
    public static TraceId parse(String hex) {
        throw new UnsupportedOperationException();
    }

    /** The id with the same 128 bits as {@code uuid}. */
    public static TraceId fromUuid(UUID uuid) {
        throw new UnsupportedOperationException();
    }

    /** The same 128 bits as a {@link UUID} (not a versioned UUID). */
    public UUID toUuid() {
        throw new UnsupportedOperationException();
    }

    /** 32 lowercase hex characters. */
    public String toHex() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(TraceId other) {
        throw new UnsupportedOperationException();
    }

    /** Same as {@link #toHex()}. */
    @Override
    public String toString() {
        throw new UnsupportedOperationException();
    }
}
