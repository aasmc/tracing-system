package ru.aasmc.tracing.collector.domain.trace;

import java.time.Instant;
import java.util.Collection;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * Header row of a trace: time bounds, root, counters and services, kept up to date as fragments of the trace arrive.
 *
 * <p>A trace reaches the collector in several fragments, in any order, possibly more than once. Each fragment adds a
 * summary of the spans it actually inserted (a replayed fragment inserts none), and summaries are combined with
 * {@link #merge}. Because {@code merge} is commutative and associative, the final summary does not depend on the
 * order of the fragments.
 *
 * <p>Rules ({@code INVALID_TRACE_SUMMARY}): {@code spanCount >= 1}; {@code 0 <= errorCount <= spanCount};
 * {@code endEpochNanos >= startEpochNanos}; {@code fragmentCount >= 1}; {@code lastIngestedAt >= firstIngestedAt};
 * {@code serviceIds} is an immutable sorted copy.
 *
 * @param traceId the trace
 * @param traceDay the day of the earliest span
 * @param startEpochNanos earliest span start
 * @param endEpochNanos latest span end
 * @param root the root span, {@code null} while it has not arrived
 * @param spanCount spans inserted
 * @param errorCount spans with status ERROR
 * @param serviceIds dictionary ids of the services involved
 * @param fragmentCount fragments that inserted at least one span
 * @param firstIngestedAt when the first span was inserted
 * @param lastIngestedAt when the last span was inserted
 */
public record TraceSummary(
        TraceId traceId,
        TraceDay traceDay,
        long startEpochNanos,
        long endEpochNanos,
        RootSpan root,
        int spanCount,
        int errorCount,
        SortedSet<Integer> serviceIds,
        int fragmentCount,
        Instant firstIngestedAt,
        Instant lastIngestedAt) {

    /**
     * Summary of the spans one fragment inserted ({@code fragmentCount = 1}).
     *
     * @param day the trace day
     * @param inserted spans of one trace, not empty
     * @param serviceIdOf dictionary id of a span's service
     * @param nameIdOf dictionary id of a span's name, {@code null} if it has none
     * @param now insert time
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_TRACE_SUMMARY} if
     *     {@code inserted} is empty or mixes traces
     */
    public static TraceSummary ofInsertedSpans(
            TraceDay day,
            Collection<Span> inserted,
            ToIntFunction<Span> serviceIdOf,
            Function<Span, Integer> nameIdOf,
            Instant now) {
        throw new UnsupportedOperationException();
    }

    /**
     * Combines two summaries of the same trace: earliest day, start and first insert; latest end and last insert;
     * sums of the counters; union of the services; and as root the one that started first (ties: the smaller
     * unsigned span id).
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_TRACE_SUMMARY} for
     *     summaries of different traces
     */
    public TraceSummary merge(TraceSummary other) {
        throw new UnsupportedOperationException();
    }
}
