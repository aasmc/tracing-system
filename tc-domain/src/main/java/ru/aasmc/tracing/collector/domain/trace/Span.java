package ru.aasmc.tracing.collector.domain.trace;

import java.util.List;
import java.util.Set;
import ru.aasmc.tracing.collector.domain.attribute.Attributes;
import ru.aasmc.tracing.collector.domain.resource.ResourceId;
import ru.aasmc.tracing.collector.domain.resource.ScopeId;

/**
 * One timed operation; immutable once stored, never updated (05 §3.6).
 *
 * <p>Invariants ({@code INVALID_SPAN}): ids not null; a span is not its own parent; {@code durationNanos >= 0} (the
 * normaliser clamps negative durations and sets {@link SpanFlag#NEGATIVE_DURATION_CLAMPED}); a {@code null} name
 * becomes {@code ""}; events, links and flags are copied into immutable collections; attributes containing a
 * truncated value require {@link SpanFlag#ATTRIBUTE_TRUNCATED}. Rules that need the clock or the trace's state
 * (future/expired start, 50 000-span limit) are in {@link IngestLimits}.
 *
 * @param traceId the trace
 * @param spanId the span
 * @param parentSpanId the parent, {@code null} for root spans
 * @param name span name ({@code ""} if absent)
 * @param kind span kind
 * @param startEpochNanos start time
 * @param durationNanos {@code end - start}, clamped to {@code >= 0}
 * @param status status
 * @param resourceId content-hash id of the resource
 * @param serviceName {@code service.name} of the resource, denormalised for the dictionary lookup
 * @param scopeId content-hash id of the instrumentation scope
 * @param traceFlags W3C trace flags
 * @param traceState W3C {@code tracestate} serialisation, or {@code null}
 * @param attributes span attributes
 * @param events span events (at most 128 kept)
 * @param links span links (at most 128 kept)
 * @param dropped counts dropped by the producer
 * @param flags derived flag bits
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
        ScopeId scopeId,
        TraceFlags traceFlags,
        String traceState,
        Attributes attributes,
        List<SpanEvent> events,
        List<SpanLinkRef> links,
        DroppedCounts dropped,
        Set<SpanFlag> flags) {

    /** {@code parentSpanId == null}. */
    public boolean isRoot() {
        throw new UnsupportedOperationException();
    }

    /** {@code startEpochNanos + durationNanos}. */
    public long endEpochNanos() {
        throw new UnsupportedOperationException();
    }

    /** UTC day of the start time. */
    public TraceDay startDay() {
        throw new UnsupportedOperationException();
    }

    public boolean hasFlag(SpanFlag flag) {
        throw new UnsupportedOperationException();
    }
}
