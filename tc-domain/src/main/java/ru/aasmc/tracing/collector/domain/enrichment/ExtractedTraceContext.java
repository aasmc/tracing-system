package ru.aasmc.tracing.collector.domain.enrichment;

import java.util.Optional;
import ru.aasmc.tracing.collector.domain.trace.SpanId;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * The trace (and optionally span) an enrichment event belongs to (05 §4.7.1).
 *
 * @param traceId the trace
 * @param spanId the span, present only for traceparent values
 */
public record ExtractedTraceContext(TraceId traceId, Optional<SpanId> spanId) {}
