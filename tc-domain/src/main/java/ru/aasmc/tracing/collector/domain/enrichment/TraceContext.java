package ru.aasmc.tracing.collector.domain.enrichment;

import ru.aasmc.tracing.collector.domain.trace.SpanId;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * The trace (and possibly span) an enrichment event refers to.
 *
 * @param traceId the trace
 * @param spanId the span, or {@code null} if the event names only the trace
 */
public record TraceContext(TraceId traceId, SpanId spanId) {}
