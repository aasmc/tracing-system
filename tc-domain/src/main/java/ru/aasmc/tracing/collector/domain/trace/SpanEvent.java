package ru.aasmc.tracing.collector.domain.trace;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/**
 * Something that happened during a span, e.g. an exception. Broken values are rejected with {@code INVALID_SPAN}.
 *
 * @param name event name, not null
 * @param epochNanos when it happened
 * @param attributes event attributes, not null
 * @param droppedAttributes attributes the producer dropped, {@code >= 0}
 */
public record SpanEvent(String name, long epochNanos, Attributes attributes, int droppedAttributes) {

    public SpanEvent {
        require(
                name != null && attributes != null && droppedAttributes >= 0,
                ViolationCode.INVALID_SPAN,
                "broken span event: " + name);
    }
}
