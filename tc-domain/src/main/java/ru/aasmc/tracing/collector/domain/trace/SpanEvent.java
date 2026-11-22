package ru.aasmc.tracing.collector.domain.trace;

import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/**
 * Something that happened during a span, e.g. an exception.
 *
 * @param name event name, not null
 * @param epochNanos when it happened
 * @param attributes event attributes, not null
 * @param droppedAttributes attributes the producer dropped, {@code >= 0}
 */
public record SpanEvent(String name, long epochNanos, Attributes attributes, int droppedAttributes) {}
