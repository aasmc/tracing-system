package ru.aasmc.tracing.collector.domain.trace;

import java.util.Set;

/** Bits of {@code span.flags smallint}; the canonical table is 04 §5.5 (05 §3.6). */
public enum SpanFlag {
    /** 0x01: an attribute value or array was cut to 4 KiB. */
    ATTRIBUTE_TRUNCATED(0x01),
    /** 0x02: {@code hasEnded = false} on the wire. */
    NOT_ENDED(0x02),
    /** 0x04: {@code parentSpanContext.remote = true}. */
    PARENT_REMOTE(0x04),
    /** 0x08: more than 128 events or links; the rest was cut. */
    EVENTS_OR_LINKS_TRUNCATED(0x08),
    /** 0x10: {@code end < start} on the wire; the duration was clamped to 0. */
    NEGATIVE_DURATION_CLAMPED(0x10),
    /** 0x20: a {@code trace_attribute.value_str} was cut to 512 bytes. */
    INDEX_VALUE_TRUNCATED(0x20);

    private final int bit;

    SpanFlag(int bit) {
        this.bit = bit;
    }

    public int bit() {
        return bit;
    }

    /** OR of the bits; {@code 0} for an empty set. */
    public static short toBits(Set<SpanFlag> flags) {
        throw new UnsupportedOperationException();
    }

    /** Inverse of {@link #toBits(Set)}; unknown bits are ignored. Returns an immutable set. */
    public static Set<SpanFlag> fromBits(short bits) {
        throw new UnsupportedOperationException();
    }
}
