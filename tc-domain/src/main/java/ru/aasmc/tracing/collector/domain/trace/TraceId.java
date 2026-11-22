package ru.aasmc.tracing.collector.domain.trace;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;
import ru.aasmc.tracing.collector.domain.DomainViolationException;
import ru.aasmc.tracing.collector.domain.ViolationCode;

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

    private static final int HEX_LENGTH = 32;

    public TraceId {
        require(high != 0 || low != 0, ViolationCode.INVALID_TRACE_ID, "trace id is all zeros");
    }

    /**
     * Parses 32 hex characters; upper case is accepted.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_TRACE_ID} for null, a wrong
     *     length, a non-hex character or all zeros
     */
    public static TraceId parse(String hex) {
        return tryParse(hex)
                .orElseThrow(
                        () -> new DomainViolationException(ViolationCode.INVALID_TRACE_ID, "not a trace id: " + hex));
    }

    /** Like {@link #parse}, but returns empty instead of throwing. */
    public static Optional<TraceId> tryParse(String hex) {
        if (hex == null || hex.length() != HEX_LENGTH || !Hex.isHex(hex, 0, HEX_LENGTH)) {
            return Optional.empty();
        }
        long high = Hex.toLong(hex, 0, 16);
        long low = Hex.toLong(hex, 16, HEX_LENGTH);
        return high == 0 && low == 0 ? Optional.empty() : Optional.of(new TraceId(high, low));
    }

    /** The id with the same 128 bits as {@code uuid}. */
    public static TraceId fromUuid(UUID uuid) {
        return new TraceId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    /** The same 128 bits as a {@link UUID} (not a versioned UUID). */
    public UUID toUuid() {
        return new UUID(high, low);
    }

    /** 32 lowercase hex characters. */
    public String toHex() {
        HexFormat hex = HexFormat.of();
        return hex.toHexDigits(high) + hex.toHexDigits(low);
    }

    @Override
    public int compareTo(TraceId other) {
        int byHigh = Long.compareUnsigned(high, other.high);
        return byHigh != 0 ? byHigh : Long.compareUnsigned(low, other.low);
    }

    /** Same as {@link #toHex()}. */
    @Override
    public String toString() {
        return toHex();
    }
}
