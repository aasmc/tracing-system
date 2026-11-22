package ru.aasmc.tracing.collector.domain.trace;

import java.util.Optional;

/**
 * 64-bit W3C span id (05 §3.2), stored as PostgreSQL {@code bigint}.
 *
 * <p>Invariants: not all zeros ({@code INVALID_SPAN_ID}). The 64 bits are reinterpreted as a two's-complement
 * {@code long}, so ids {@code >= 8000000000000000} become negative; the mapping is lossless. The all-zero parent id of
 * the wire means "no parent" and is handled by {@link #parseParent(String)}, not by the constructor.
 *
 * @param value the 64 bits as a signed long
 */
public record SpanId(long value) {

    /** The wire's parent id of a root span. */
    public static final String ROOT_PARENT_HEX = "0000000000000000";

    /** Number of hex characters of the text form. */
    public static final int HEX_LENGTH = 16;

    /**
     * Parses 16 hex characters (upper case accepted).
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_SPAN_ID}
     */
    public static SpanId parse(String hex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Parses a parent id: {@link #ROOT_PARENT_HEX}, {@code null} and {@code ""} give {@link Optional#empty()} (root
     * span); anything else is parsed like {@link #parse(String)}.
     */
    public static Optional<SpanId> parseParent(String hex) {
        throw new UnsupportedOperationException();
    }

    /** 16 lowercase hex characters. */
    public String toHex() {
        throw new UnsupportedOperationException();
    }

    /** Same as {@link #toHex()}. */
    @Override
    public String toString() {
        throw new UnsupportedOperationException();
    }
}
