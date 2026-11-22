package ru.aasmc.tracing.collector.domain.trace;

import java.util.Optional;

/**
 * A 64-bit W3C span id. Its text form is 16 lowercase hex characters; in PostgreSQL it is a signed {@code bigint},
 * so ids from {@code 8000000000000000} up are stored as negative numbers. The mapping is lossless.
 *
 * <p>Never zero.
 *
 * @param value the 64 bits as a signed long
 */
public record SpanId(long value) {

    /**
     * Parses 16 hex characters; upper case is accepted.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_SPAN_ID} for null, a wrong
     *     length, a non-hex character or all zeros
     */
    public static SpanId parse(String hex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Parses a parent span id. Producers send {@code 0000000000000000} (or nothing) for a root span, so that value,
     * {@code null} and {@code ""} give an empty result; anything else is parsed like {@link #parse}.
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
