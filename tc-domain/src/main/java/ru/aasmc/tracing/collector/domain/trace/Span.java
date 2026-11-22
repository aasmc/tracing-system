package ru.aasmc.tracing.collector.domain.trace;

import java.util.List;
import java.util.Set;
import ru.aasmc.tracing.collector.domain.attribute.Attributes;
import ru.aasmc.tracing.collector.domain.resource.InstrumentationScopeId;
import ru.aasmc.tracing.collector.domain.resource.ResourceId;

/**
 * One timed operation of a trace. Stored once and never updated.
 *
 * <p>Rules ({@code INVALID_SPAN}): ids are set and a span is not its own parent; {@code durationNanos >= 0}; a
 * {@code null} name becomes {@code ""}; events, links and collector flags are copied into immutable collections; if an
 * attribute value was cut, {@link SpanFlag#ATTRIBUTE_TRUNCATED} is set. Rules that depend on the clock or on other
 * spans of the trace are in {@link IngestLimits}.
 *
 * @param traceId the trace
 * @param spanId the span
 * @param parentSpanId the parent, {@code null} for a root span
 * @param name operation name
 * @param kind span kind
 * @param startEpochNanos start time
 * @param durationNanos end minus start
 * @param status status
 * @param resourceId the process that produced the span (see {@link ru.aasmc.tracing.collector.domain.resource.ResourceHasher})
 * @param serviceName {@code service.name} of that process
 * @param instrumentationScopeId the instrumentation library that produced the span
 * @param traceFlags W3C trace flags
 * @param traceState W3C {@code tracestate}, or {@code null}
 * @param attributes span attributes
 * @param events span events
 * @param links links to other spans
 * @param droppedByProducer what the producer dropped
 * @param collectorFlags what the collector noticed while normalising the span
 */
public record Span(
        TraceId traceId,
        SpanId spanId,
        SpanId parentSpanId,
        String name,
        SpanKind kind,
        long startEpochNanos,
        long durationNanos,
        SpanStatus status,
        ResourceId resourceId,
        String serviceName,
        InstrumentationScopeId instrumentationScopeId,
        TraceFlags traceFlags,
        String traceState,
        Attributes attributes,
        List<SpanEvent> events,
        List<SpanLink> links,
        DroppedCounts droppedByProducer,
        Set<SpanFlag> collectorFlags) {

    public boolean isRoot() {
        throw new UnsupportedOperationException();
    }

    public long endEpochNanos() {
        throw new UnsupportedOperationException();
    }
}
