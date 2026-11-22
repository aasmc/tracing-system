package ru.aasmc.tracing.collector.testmodel.domain;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import ru.aasmc.tracing.collector.domain.trace.RootSpan;
import ru.aasmc.tracing.collector.domain.trace.Span;
import ru.aasmc.tracing.collector.domain.trace.StatusCode;
import ru.aasmc.tracing.collector.domain.trace.TraceDay;
import ru.aasmc.tracing.collector.domain.trace.TraceSummary;

/**
 * Builds trace summaries. Defaults: a valid rootless summary of one span of service 1, one fragment, 1 ms long.
 *
 * <p>{@link #expectedOf} computes the summary a set of spans must end up with, without using production code, so
 * tests can check that summaries merged in any order equal it.
 */
public final class TraceSummaryBuilder {

    private static final Instant INGESTED = Instant.parse("2026-11-21T10:00:00Z");

    private long start = TestTime.epochNanos("2026-11-21T09:00:00Z");
    private long end = start + 1_000_000;
    private int spanCount = 1;
    private int errorCount;
    private int fragmentCount = 1;
    private Instant firstIngestedAt = INGESTED;
    private Instant lastIngestedAt = INGESTED;

    public static TraceSummaryBuilder aSummary() {
        return new TraceSummaryBuilder();
    }

    /**
     * The summary of {@code spans} (one trace) after all were inserted: day of the earliest start, bounds, counters,
     * services, and as root the parentless span that started first (ties: smaller unsigned span id).
     */
    public static TraceSummary expectedOf(
            Collection<Span> spans,
            Map<String, Integer> serviceIdByName,
            Map<String, Integer> nameIdByName,
            int fragmentCount,
            Instant ingestedAt) {
        long start = spans.stream().mapToLong(Span::startEpochNanos).min().orElseThrow();
        long end = spans.stream()
                .mapToLong(s -> s.startEpochNanos() + s.durationNanos())
                .max()
                .orElseThrow();
        int errors = (int) spans.stream()
                .filter(s -> s.status().code() == StatusCode.ERROR)
                .count();
        SortedSet<Integer> services = new TreeSet<>();
        spans.forEach(s -> services.add(serviceIdByName.get(s.serviceName())));
        RootSpan root = spans.stream()
                .filter(s -> s.parentSpanId() == null)
                .min(Comparator.comparingLong(Span::startEpochNanos)
                        .thenComparing(s -> s.spanId().value(), Long::compareUnsigned))
                .map(s -> new RootSpan(
                        s.spanId(),
                        serviceIdByName.get(s.serviceName()),
                        nameIdByName.get(s.name()),
                        s.startEpochNanos()))
                .orElse(null);
        return new TraceSummary(
                spans.iterator().next().traceId(),
                dayOf(start),
                start,
                end,
                root,
                spans.size(),
                errors,
                Collections.unmodifiableSortedSet(services),
                fragmentCount,
                ingestedAt,
                ingestedAt);
    }

    public TraceSummaryBuilder bounds(long startNanos, long endNanos) {
        start = startNanos;
        end = endNanos;
        return this;
    }

    public TraceSummaryBuilder spanCount(int value) {
        spanCount = value;
        return this;
    }

    public TraceSummaryBuilder errorCount(int value) {
        errorCount = value;
        return this;
    }

    public TraceSummaryBuilder fragmentCount(int value) {
        fragmentCount = value;
        return this;
    }

    public TraceSummaryBuilder ingested(Instant first, Instant last) {
        firstIngestedAt = first;
        lastIngestedAt = last;
        return this;
    }

    public TraceSummary build() {
        return new TraceSummary(
                DomainIds.traceId("0af7651916cd43dd8448eb211c80319c"),
                dayOf(start),
                start,
                end,
                null,
                spanCount,
                errorCount,
                Collections.unmodifiableSortedSet(new TreeSet<>(Set.of(1))),
                fragmentCount,
                firstIngestedAt,
                lastIngestedAt);
    }

    private static TraceDay dayOf(long epochNanos) {
        return new TraceDay(LocalDate.ofInstant(TestTime.instantOf(epochNanos), ZoneOffset.UTC));
    }
}
