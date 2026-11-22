package ru.aasmc.tracing.collector.domain.trace;

/**
 * W3C trace flags (05 §3.6). The wire sends a signed Java byte; stored as {@code smallint} 0..255.
 *
 * <p>Invariants: {@code 0 <= value <= 255}.
 *
 * @param value the unsigned flag byte
 */
public record TraceFlags(int value) {

    /** Maps the wire's signed byte with {@code b & 0xFF}. */
    public static TraceFlags fromWireByte(byte b) {
        throw new UnsupportedOperationException();
    }

    /** Bit 0x01. */
    public boolean sampled() {
        throw new UnsupportedOperationException();
    }
}
