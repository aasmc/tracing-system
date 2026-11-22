package ru.aasmc.tracing.collector.domain.trace;

import java.util.UUID;

/**
 * 128-bit W3C trace id (05 §3.1). Stored as PostgreSQL {@code uuid}, rendered as 32 lowercase hex characters.
 *
 * <p>Invariants: not all zeros ({@code INVALID_TRACE_ID}). {@link #toUuid()} and {@link #fromUuid(UUID)} are exact
 * inverses. {@link #compareTo} is unsigned byte order, the same as PostgreSQL's {@code uuid} ordering, so keyset
 * cursors built in Java agree with {@code ORDER BY trace_id}. Immutable and thread-safe.
 *
 * @param high the most significant 64 bits
 * @param low the least significant 64 bits
 */
public record TraceId(long high, long low) implements Comparable<TraceId> {

    /** Number of hex characters of the text form. */
    public static final int HEX_LENGTH = 32;

    /**
     * Parses 32 hex characters. Upper case is accepted on API input; the wire always sends lower case.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_TRACE_ID} on null, wrong
     *     length, non-hex characters or all zeros
     */
    public static TraceId parse(String hex) {
        throw new UnsupportedOperationException();
    }

    /** Inverse of {@link #toUuid()}. The UUID is raw 128 bits, not an RFC 9562 versioned UUID. */
    public static TraceId fromUuid(UUID uuid) {
        throw new UnsupportedOperationException();
    }

    /** Raw 128 bits as a UUID, for the {@code uuid} column. */
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
