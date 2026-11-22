package ru.aasmc.tracing.collector.domain.trace;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * W3C trace flags: one unsigned byte, bit {@code 0x01} meaning "sampled". Unknown bits are kept. A value outside
 * 0..255 is rejected with {@code INVALID_SPAN}.
 *
 * @param value 0..255
 */
public record TraceFlags(int value) {

    public TraceFlags {
        require(value >= 0 && value <= 0xff, ViolationCode.INVALID_SPAN, "trace flags outside 0..255: " + value);
    }
}
