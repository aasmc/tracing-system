package ru.aasmc.tracing.collector.domain.trace;

/**
 * W3C trace flags: one unsigned byte, bit {@code 0x01} meaning "sampled". Unknown bits are kept.
 *
 * @param value 0..255
 */
public record TraceFlags(int value) {}
