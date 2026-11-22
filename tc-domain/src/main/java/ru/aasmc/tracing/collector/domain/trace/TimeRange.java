package ru.aasmc.tracing.collector.domain.trace;

import java.time.Instant;

/**
 * A half-open time interval {@code [from, to)}.
 *
 * <p>Both ends are set and {@code from < to} ({@code INVALID_TIME_RANGE}).
 *
 * @param from inclusive start
 * @param to exclusive end
 */
public record TimeRange(Instant from, Instant to) {}
