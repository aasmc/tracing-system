package ru.aasmc.tracing.collector.domain.trace;

import java.time.Instant;
import java.util.Collection;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * Trace-level header, one row per {@code (trace_id, trace_day)} of {@code trace_summary} (05 §4.4).
 *
 * <p>A commutative monoid over the set of <em>inserted</em> spans: {@code ofInsertedSpans(S1).merge(
 * ofInsertedSpans(S2))} equals the summary of {@code S1 ∪ S2} for disjoint sets (counters, bounds, root, services),
 * and {@link #merge} is commutative and associative. A replayed fragment inserts no spans and contributes nothing.
 *
 * <p>Invariants ({@code INVALID_TRACE_SUMMARY}): {@code spanCount >= 1}; {@code 0 <= errorCount <= spanCount};
 * {@code end >= start}; {@code fragmentCount >= 1}; {@code lastIngestedAt >= firstIngestedAt}; {@code serviceIds} is
 * an immutable sorted copy.
 *
 * @param traceId the trace
 * @param traceDay partition key
 * @param startEpochNanos min span start
 * @param endEpochNanos max span end
 * @param root root span, {@code null} while rootless
 * @param spanCount inserted spans
 * @param errorCount inserted spans with status ERROR
 * @param serviceIds distinct service dictionary ids
 * @param fragmentCount fragments that inserted at least one span
 * @param firstIngestedAt first insert time
 * @param lastIngestedAt last insert time
 */
public record TraceSummary(
        TraceId traceId,
        TraceDay traceDay,
        long startEpochNanos,
        long endEpochNanos,
        RootInfo root,
        int spanCount,
        int errorCount,
        SortedSet<Integer> serviceIds,
        int fragmentCount,
        Instant firstIngestedAt,
        Instant lastIngestedAt) {

    /**
     * Summary of the spans actually inserted from one fragment ({@code fragmentCount = 1}).
     *
     * @param day the resolved trace day
     * @param inserted non-empty spans of one trace
     * @param serviceIdOf dictionary id of a span's service
     * @param nameIdOf dictionary id of a span's name, {@code null} on overflow
     * @param now ingest time
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
     * Commutative, associative merge that mirrors the SQL {@code ON CONFLICT DO UPDATE} of {@code trace_summary}:
     * min/max of bounds and ingest times, sums of counters, union of services, the earlier trace day (read-side merge
     * of split rows), and the root with the earliest start, then the smallest unsigned span id.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_TRACE_SUMMARY} for summaries
     *     of different traces
     */
    public TraceSummary merge(TraceSummary other) {
        throw new UnsupportedOperationException();
    }

    public long durationNanos() {
        throw new UnsupportedOperationException();
    }

    public boolean rootless() {
        throw new UnsupportedOperationException();
    }
}
