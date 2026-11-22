package ru.aasmc.tracing.collector.domain.trace;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.util.HexFormat;
import java.util.Optional;
import ru.aasmc.tracing.collector.domain.DomainViolationException;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * A 64-bit W3C span id. Its text form is 16 lowercase hex characters; in PostgreSQL it is a signed {@code bigint},
 * so ids from {@code 8000000000000000} up are stored as negative numbers. The mapping is lossless.
 *
 * <p>Never zero.
 *
 * @param value the 64 bits as a signed long
 */
public record SpanId(long value) {

    private static final int HEX_LENGTH = 16;
    private static final String ROOT_PARENT = "0000000000000000";

    public SpanId {
        require(value != 0, ViolationCode.INVALID_SPAN_ID, "span id is zero");
    }

    /**
     * Parses 16 hex characters; upper case is accepted.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_SPAN_ID} for null, a wrong
     *     length, a non-hex character or all zeros
     */
    public static SpanId parse(String hex) {
        return tryParse(hex)
                .orElseThrow(
                        () -> new DomainViolationException(ViolationCode.INVALID_SPAN_ID, "not a span id: " + hex));
    }

    /** Like {@link #parse}, but returns empty instead of throwing. */
    static Optional<SpanId> tryParse(String hex) {
        if (hex == null || hex.length() != HEX_LENGTH || !Hex.isHex(hex, 0, HEX_LENGTH)) {
            return Optional.empty();
        }
        long value = Hex.toLong(hex, 0, HEX_LENGTH);
        return value == 0 ? Optional.empty() : Optional.of(new SpanId(value));
    }

    /**
     * Parses a parent span id. Producers send {@code 0000000000000000} (or nothing) for a root span, so that value,
     * {@code null} and {@code ""} give an empty result; anything else is parsed like {@link #parse}.
     */
    public static Optional<SpanId> parseParent(String hex) {
        if (hex == null || hex.isEmpty() || hex.equals(ROOT_PARENT)) {
            return Optional.empty();
        }
        return Optional.of(parse(hex));
    }

    /** 16 lowercase hex characters. */
    public String toHex() {
        return HexFormat.of().toHexDigits(value);
    }

    /** Same as {@link #toHex()}. */
    @Override
    public String toString() {
        return toHex();
    }
}
