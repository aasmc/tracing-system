package ru.aasmc.tracing.collector.domain.trace;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import ru.aasmc.tracing.collector.domain.ViolationCode;

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

    /** Earlier start first, then the smaller unsigned span id. */
    private static final Comparator<RootSpan> ROOT_ORDER = Comparator.comparingLong(RootSpan::startEpochNanos)
            .thenComparing(root -> root.spanId().value(), Long::compareUnsigned);

    public TraceSummary {
        require(spanCount >= 1, ViolationCode.INVALID_TRACE_SUMMARY, "no spans: " + spanCount);
        require(
                errorCount >= 0 && errorCount <= spanCount,
                ViolationCode.INVALID_TRACE_SUMMARY,
                errorCount + " errors in " + spanCount + " spans");
        require(endEpochNanos >= startEpochNanos, ViolationCode.INVALID_TRACE_SUMMARY, "end before start");
        require(fragmentCount >= 1, ViolationCode.INVALID_TRACE_SUMMARY, "no fragments: " + fragmentCount);
        require(
                !lastIngestedAt.isBefore(firstIngestedAt),
                ViolationCode.INVALID_TRACE_SUMMARY,
                "last insert before first");
        serviceIds = Collections.unmodifiableSortedSet(new TreeSet<>(serviceIds));
    }

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
        require(!inserted.isEmpty(), ViolationCode.INVALID_TRACE_SUMMARY, "no inserted spans");
        TraceId traceId = inserted.iterator().next().traceId();
        require(
                inserted.stream().allMatch(span -> span.traceId().equals(traceId)),
                ViolationCode.INVALID_TRACE_SUMMARY,
                "spans of several traces");
        SortedSet<Integer> serviceIds = new TreeSet<>();
        inserted.forEach(span -> serviceIds.add(serviceIdOf.applyAsInt(span)));
        RootSpan root = inserted.stream()
                .filter(Span::isRoot)
                .map(span -> new RootSpan(
                        span.spanId(), serviceIdOf.applyAsInt(span), nameIdOf.apply(span), span.startEpochNanos()))
                .min(ROOT_ORDER)
                .orElse(null);
        return new TraceSummary(
                traceId,
                day,
                inserted.stream().mapToLong(Span::startEpochNanos).min().orElseThrow(),
                inserted.stream().mapToLong(Span::endEpochNanos).max().orElseThrow(),
                root,
                inserted.size(),
                (int) inserted.stream().filter(span -> span.status().isError()).count(),
                serviceIds,
                1,
                now,
                now);
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
        require(
                traceId.equals(other.traceId),
                ViolationCode.INVALID_TRACE_SUMMARY,
                "cannot merge traces " + traceId + " and " + other.traceId);
        SortedSet<Integer> allServiceIds = new TreeSet<>(serviceIds);
        allServiceIds.addAll(other.serviceIds);
        return new TraceSummary(
                traceId,
                traceDay.compareTo(other.traceDay) <= 0 ? traceDay : other.traceDay,
                Math.min(startEpochNanos, other.startEpochNanos),
                Math.max(endEpochNanos, other.endEpochNanos),
                earlierRoot(root, other.root),
                spanCount + other.spanCount,
                errorCount + other.errorCount,
                allServiceIds,
                fragmentCount + other.fragmentCount,
                firstIngestedAt.isBefore(other.firstIngestedAt) ? firstIngestedAt : other.firstIngestedAt,
                lastIngestedAt.isAfter(other.lastIngestedAt) ? lastIngestedAt : other.lastIngestedAt);
    }

    private static RootSpan earlierRoot(RootSpan a, RootSpan b) {
        if (a == null || b == null) {
            return a == null ? b : a;
        }
        return ROOT_ORDER.compare(a, b) <= 0 ? a : b;
    }
}
