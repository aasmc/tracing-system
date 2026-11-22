package ru.aasmc.tracing.collector.domain.trace;

/** What ingestion does with a span, judged by its start time. */
public enum SpanTimeVerdict {
    /** Store the span. */
    ACCEPT,
    /** Starts too far in the future: the whole message is rejected as broken. */
    FUTURE,
    /** Older than the retention period: drop this span, keep the others. */
    EXPIRED
}
