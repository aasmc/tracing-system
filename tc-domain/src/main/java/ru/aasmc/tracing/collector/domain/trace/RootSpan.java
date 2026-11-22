package ru.aasmc.tracing.collector.domain.trace;

/**
 * The root span of a trace (the span without a parent), as kept in the trace summary. A broken trace may have several
 * roots; the summary keeps the one that started first.
 *
 * @param spanId root span id
 * @param serviceId dictionary id of the root's service
 * @param nameId dictionary id of the root's name, or {@code null} if the name is not in the dictionary
 * @param startEpochNanos when the root span started
 */
public record RootSpan(SpanId spanId, int serviceId, Integer nameId, long startEpochNanos) {}
