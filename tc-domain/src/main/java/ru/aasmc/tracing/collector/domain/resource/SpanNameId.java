package ru.aasmc.tracing.collector.domain.resource;

/**
 * Dictionary id of a span name within one service  ({@code tc.span_name}, 05 §3.5). Invariant: {@code value > 0}.
 *
 * @param value the id
 */
public record SpanNameId(int value) {}
