package ru.aasmc.tracing.collector.domain.trace;

import java.time.Duration;
import java.time.Instant;
import ru.aasmc.tracing.collector.domain.Limits;
import ru.aasmc.tracing.collector.domain.attribute.AttributeLimits;
import ru.aasmc.tracing.collector.domain.attribute.TruncatedText;

/**
 * Configurable limits that protect storage from oversized or bogus data. A value exactly at a limit is accepted; one
 * more is dropped, cut or rejected. All limits are positive; {@link #DEFAULT} holds the values used when none are
 * configured.
 *
 * @param maxSpansPerTrace spans stored per trace; later spans of the trace are dropped
 * @param maxFragmentBytes size of one Kafka message; a larger message is rejected as broken
 * @param futureTolerance how far in the future a span may start (producer clocks drift); later is rejected
 * @param maxIndexValueBytes UTF-8 bytes of a value in the search index; longer values are cut there
 * @param attributes limits of the attributes of spans, events, links, resources and scopes
 */
public record IngestLimits(
        int maxSpansPerTrace,
        int maxFragmentBytes,
        Duration futureTolerance,
        int maxIndexValueBytes,
        AttributeLimits attributes) {

    public static final IngestLimits DEFAULT =
            new IngestLimits(50_000, 8 * 1024 * 1024, Duration.ofDays(1), 512, AttributeLimits.DEFAULT);

    public IngestLimits {
        Limits.requirePositive("maxSpansPerTrace", maxSpansPerTrace);
        Limits.requirePositive("maxFragmentBytes", maxFragmentBytes);
        Limits.requirePositive("futureTolerance", futureTolerance);
        Limits.requirePositive("maxIndexValueBytes", maxIndexValueBytes);
        if (attributes == null) {
            throw new IllegalArgumentException("attribute limits are missing");
        }
    }

    /**
     * How many of {@code incoming} new spans of a trace may still be stored when {@code alreadyStored} are.
     *
     * @return a number in {@code [0, incoming]}
     */
    public int spansToStore(int alreadyStored, int incoming) {
        return Math.max(0, Math.min(incoming, maxSpansPerTrace - alreadyStored));
    }

    public boolean isFragmentTooLarge(int encodedBytes) {
        return encodedBytes > maxFragmentBytes;
    }

    /**
     * Judges a span by its start: {@link SpanTimeVerdict#FUTURE} if after {@code now + futureTolerance},
     * {@link SpanTimeVerdict#EXPIRED} if before {@code now - retention}, else {@link SpanTimeVerdict#ACCEPT}.
     *
     * @param retention how long full trace data is kept
     */
    public SpanTimeVerdict judgeSpanStart(long startEpochNanos, Instant now, Duration retention) {
        Instant start = EpochNanos.toInstant(startEpochNanos);
        if (start.isAfter(now.plus(futureTolerance))) {
            return SpanTimeVerdict.FUTURE;
        }
        if (start.isBefore(now.minus(retention))) {
            return SpanTimeVerdict.EXPIRED;
        }
        return SpanTimeVerdict.ACCEPT;
    }

    /** Cuts a value to {@link #maxIndexValueBytes} without splitting a character. */
    public TruncatedText truncateIndexValue(String value) {
        return TruncatedText.truncate(value, maxIndexValueBytes);
    }
}
