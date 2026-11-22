package ru.aasmc.tracing.collector.domain.trace;

import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/**
 * A native OTel link from a span to a span of another (or the same) trace (05 §1 "Span Link", §6.2). Written to
 * {@code span.links} jsonb and, for inserted spans, to {@code span_link} for lookups in both directions.
 *
 * <p>Invariants: linked ids not null; {@code attributes} not null; {@code traceState} is the W3C serialisation or
 * {@code null}.
 *
 * @param linkedTraceId trace of the linked span
 * @param linkedSpanId the linked span
 * @param traceState W3C {@code tracestate} of the link, or {@code null}
 * @param attributes link attributes
 */
public record SpanLinkRef(TraceId linkedTraceId, SpanId linkedSpanId, String traceState, Attributes attributes) {}
