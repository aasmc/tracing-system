package ru.aasmc.tracing.collector.domain.trace;

/**
 * The root span as seen by the trace summary (05 §4.4).
 *
 * @param spanId root span id
 * @param serviceId dictionary id of the root's service
 * @param nameId dictionary id of the root's name, {@code null} if the name overflowed the per-service dictionary
 * @param startEpochNanos root start, used to pick the earliest root of a malformed multi-root trace
 */
public record RootInfo(SpanId spanId, int serviceId, Integer nameId, long startEpochNanos) {}
