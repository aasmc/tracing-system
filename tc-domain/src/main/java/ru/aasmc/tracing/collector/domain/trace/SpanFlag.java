package ru.aasmc.tracing.collector.domain.trace;

/** Facts the collector records about a span while normalising it. */
public enum SpanFlag {
    /** An attribute value was longer than 4 KiB and was cut. */
    ATTRIBUTE_TRUNCATED,
    /** The span had no end time; its duration is 0. */
    NOT_ENDED,
    /** The parent span lives in another process. */
    PARENT_REMOTE,
    /** More events or links than the per-span limit were sent; the rest were dropped. */
    EVENTS_OR_LINKS_TRUNCATED,
    /** The end was before the start; the duration was set to 0. */
    NEGATIVE_DURATION_CLAMPED,
    /** A value was longer than the search index allows and was cut in the index (not in the span). */
    INDEX_VALUE_TRUNCATED
}
