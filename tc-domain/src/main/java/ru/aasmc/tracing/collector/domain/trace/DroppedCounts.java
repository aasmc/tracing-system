package ru.aasmc.tracing.collector.domain.trace;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * How many attributes, events and links the producer dropped from a span because of its own limits. A negative
 * count is rejected with {@code INVALID_SPAN}.
 *
 * @param attributes dropped attributes, {@code >= 0}
 * @param events dropped events, {@code >= 0}
 * @param links dropped links, {@code >= 0}
 */
public record DroppedCounts(int attributes, int events, int links) {

    public static final DroppedCounts NONE = new DroppedCounts(0, 0, 0);

    public DroppedCounts {
        require(
                attributes >= 0 && events >= 0 && links >= 0,
                ViolationCode.INVALID_SPAN,
                "negative dropped count: " + attributes + ", " + events + ", " + links);
    }
}
