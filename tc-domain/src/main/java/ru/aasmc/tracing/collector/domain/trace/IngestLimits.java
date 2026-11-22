package ru.aasmc.tracing.collector.domain.trace;

import java.time.Duration;
import java.time.Instant;
import ru.aasmc.tracing.collector.domain.attribute.TruncatedText;

/**
 * Ingest limits (05 §3.6, 03 §2 table "Limits", 04 §5.5). Boundaries are inclusive: a value exactly at the limit
 * passes, limit + 1 is cut, dropped or rejected.
 *
 * <p>Invariants: every limit is positive.
 *
 * @param maxSpansPerTrace spans kept per trace (default 50 000); excess spans are dropped ({@code TRACE_TOO_LARGE})
 * @param maxAttributeValueBytes UTF-8 bytes of a string attribute value (default 4 KiB); longer values are cut and
 *     flagged {@link SpanFlag#ATTRIBUTE_TRUNCATED}
 * @param maxFragmentBytes encoded bytes of one record (default 8 MiB); larger records are poison
 * @param futureTolerance how far a span may start in the future (default 1 day); later is poison
 * @param maxIndexValueBytes UTF-8 bytes of {@code trace_attribute.value_str} (default 512); longer values are cut
 *     and flagged {@link SpanFlag#INDEX_VALUE_TRUNCATED}
 * @param maxEventsPerSpan events kept per span (default 128)
 * @param maxLinksPerSpan links kept per span (default 128)
 */
public record IngestLimits(
        int maxSpansPerTrace,
        int maxAttributeValueBytes,
        int maxFragmentBytes,
        Duration futureTolerance,
        int maxIndexValueBytes,
        int maxEventsPerSpan,
        int maxLinksPerSpan) {

    public static final IngestLimits DEFAULT =
            new IngestLimits(50_000, 4 * 1024, TraceFragment.MAX_BYTES, Duration.ofDays(1), 512, 128, 128);

    /**
     * How many of {@code incoming} new spans of a trace may still be stored, given {@code alreadyStored} spans
     * ({@code trace_summary.span_count} plus earlier spans of the same batch). Result in {@code [0, incoming]}.
     */
    public int spansAdmitted(int alreadyStored, int incoming) {
        throw new UnsupportedOperationException();
    }

    /** {@code encodedBytes > maxFragmentBytes}. */
    public boolean fragmentTooLarge(int encodedBytes) {
        throw new UnsupportedOperationException();
    }

    /**
     * Classifies a span start against {@code now} and the full-tier retention.
     *
     * @param fullRetention {@code RetentionTiers.fullRetention()}
     */
    public SpanTimeVerdict classifyStart(long startEpochNanos, Instant now, Duration fullRetention) {
        throw new UnsupportedOperationException();
    }

    /** Cuts an attribute string to {@link #maxAttributeValueBytes} without splitting a code point. */
    public TruncatedText truncateAttributeValue(String value) {
        throw new UnsupportedOperationException();
    }

    /** Cuts an index token to {@link #maxIndexValueBytes} without splitting a code point. */
    public TruncatedText truncateIndexValue(String token) {
        throw new UnsupportedOperationException();
    }
}
