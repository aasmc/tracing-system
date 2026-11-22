package ru.aasmc.tracing.collector.domain.trace;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.time.Instant;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * A half-open time interval {@code [from, to)}.
 *
 * <p>Both ends are set and {@code from < to} ({@code INVALID_TIME_RANGE}).
 *
 * @param from inclusive start
 * @param to exclusive end
 */
public record TimeRange(Instant from, Instant to) {

    public TimeRange {
        require(
                from != null && to != null && from.isBefore(to),
                ViolationCode.INVALID_TIME_RANGE,
                "not a time range: [" + from + ", " + to + ")");
    }
}
