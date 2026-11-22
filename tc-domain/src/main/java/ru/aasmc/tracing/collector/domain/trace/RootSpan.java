package ru.aasmc.tracing.collector.domain.trace;

/**
 * The root span of a trace, as kept in the trace summary.
 *
 * @param spanId root span id
 * @param serviceId dictionary id of the root's service
 * @param nameId dictionary id of the root's name, or {@code null} if the name is not in the dictionary
 * @param startEpochNanos root start; if a broken trace has several roots, the earliest one wins
 */
public record RootSpan(SpanId spanId, int serviceId, Integer nameId, long startEpochNanos) {}
