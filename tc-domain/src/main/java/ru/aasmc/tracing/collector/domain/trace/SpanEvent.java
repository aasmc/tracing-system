package ru.aasmc.tracing.collector.domain.trace;

import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/**
 * A span event (05 §3.6, §6.2), stored inside {@code span.events} jsonb.
 *
 * <p>Deviation from 05 (which had an offset): the event carries its absolute time in epoch nanoseconds, exactly as
 * the wire and the stored jsonb ({@code "time"}) do. Invariants: {@code name} not null, {@code attributes} not null,
 * {@code droppedAttributes >= 0}.
 *
 * @param name event name
 * @param epochNanos event time
 * @param attributes event attributes
 * @param droppedAttributes attributes dropped by the producer
 */
public record SpanEvent(String name, long epochNanos, Attributes attributes, int droppedAttributes) {}
