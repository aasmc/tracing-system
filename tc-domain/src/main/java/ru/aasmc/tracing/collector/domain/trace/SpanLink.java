package ru.aasmc.tracing.collector.domain.trace;

import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/**
 * A link from a span to another span, possibly in another trace (e.g. a batch job linking the requests it handles).
 *
 * @param linkedTraceId trace of the linked span, not null
 * @param linkedSpanId the linked span, not null
 * @param traceState W3C {@code tracestate} of the link, or {@code null}
 * @param attributes link attributes, not null
 */
public record SpanLink(TraceId linkedTraceId, SpanId linkedSpanId, String traceState, Attributes attributes) {}
