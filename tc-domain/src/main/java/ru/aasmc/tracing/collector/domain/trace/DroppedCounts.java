package ru.aasmc.tracing.collector.domain.trace;

/**
 * How many attributes, events and links the producer dropped from a span because of its own limits.
 *
 * @param attributes dropped attributes, {@code >= 0}
 * @param events dropped events, {@code >= 0}
 * @param links dropped links, {@code >= 0}
 */
public record DroppedCounts(int attributes, int events, int links) {

    public static final DroppedCounts NONE = new DroppedCounts(0, 0, 0);
}
