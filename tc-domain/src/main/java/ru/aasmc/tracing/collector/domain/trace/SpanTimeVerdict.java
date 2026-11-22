package ru.aasmc.tracing.collector.domain.trace;

/** Result of checking a span start against the clock (05 §3.6 rules 4 and 5). */
public enum SpanTimeVerdict {
    /** Inside {@code [now - fullDays, now + futureTolerance]}. */
    ACCEPT,
    /** After {@code now + futureTolerance}: the whole record is poison ({@code FUTURE_TIMESTAMP}, DLQ). */
    FUTURE,
    /** Before {@code now - fullDays}: the span is dropped (metric reason {@code EXPIRED}), the rest proceeds. */
    EXPIRED
}
