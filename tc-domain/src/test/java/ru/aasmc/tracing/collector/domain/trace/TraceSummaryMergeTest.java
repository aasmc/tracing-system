package ru.aasmc.tracing.collector.domain.trace;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.GATEWAY;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.NAME_IDS;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.SERVICE_IDS;
import static ru.aasmc.tracing.collector.testmodel.domain.SpanBuilder.aSpan;
import static ru.aasmc.tracing.collector.testmodel.domain.TraceSummaryBuilder.aSummary;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.testmodel.domain.IdpScenario;
import ru.aasmc.tracing.collector.testmodel.domain.Permutations;
import ru.aasmc.tracing.collector.testmodel.domain.TimeEdgeCases;
import ru.aasmc.tracing.collector.testmodel.domain.TraceSummaryBuilder;

class TraceSummaryMergeTest {

    private static final Instant INGESTED = Instant.parse("2026-11-21T10:00:00Z");
    private static final Instant INGESTED_LATER = INGESTED.plusSeconds(30);

    /** Service id for spans of services outside the IdP scenario. */
    private static final int OTHER_SERVICE_ID = 9;

    private static final Map<String, TraceId> IDP_TRACES =
            Map.of("T1", IdpScenario.T1, "T2", IdpScenario.T2, "T3", IdpScenario.T3);

    static Stream<Arguments> idpTracesAndSeeds() {
        return IDP_TRACES.keySet().stream()
                .sorted()
                .flatMap(trace -> LongStream.rangeClosed(1, 5).mapToObj(seed -> Arguments.of(trace, seed)));
    }

    @ParameterizedTest(name = "{0}, seed {1}")
    @MethodSource("idpTracesAndSeeds")
    @DisplayName("fragments merged in any order and grouping give the summary of the whole trace")
    void given_randomFragments_when_mergedInAnyOrder_then_summaryOfWholeTrace(String trace, long seed) {
        List<Span> spans = new ArrayList<>(IdpScenario.spans(IDP_TRACES.get(trace)));
        Collections.shuffle(spans, new Random(seed));
        List<TraceSummary> fragments = Permutations.randomSplits(spans, seed).stream()
                .map(fragment -> summaryOf(fragment, INGESTED))
                .toList();
        TraceSummary expected =
                TraceSummaryBuilder.expectedOf(spans, SERVICE_IDS, NAME_IDS, fragments.size(), INGESTED);

        for (List<TraceSummary> order : Permutations.allOrders(fragments)) {
            assertThat(mergeFromLeft(order)).isEqualTo(expected);
            assertThat(mergeFromRight(order)).isEqualTo(expected);
        }
    }

    @Test
    @DisplayName("merging keeps the root from whichever fragment carries it and unites the services")
    void given_rootInLaterFragment_when_merged_then_rootServicesAndIngestTimes() {
        List<Span> spans = IdpScenario.spans(IdpScenario.T2);
        Span root = spans.getFirst();
        TraceSummary withoutRoot = summaryOf(spans.subList(1, 3), INGESTED);
        TraceSummary withRoot = summaryOf(List.of(root), INGESTED_LATER);

        TraceSummary merged = withoutRoot.merge(withRoot);

        assertThat(withoutRoot.root()).isNull();
        assertThat(merged.root()).isEqualTo(new RootSpan(root.spanId(), 1, 2, root.startEpochNanos()));
        assertThat(merged.serviceIds()).containsExactly(1, 2, 3);
        assertThat(merged.spanCount()).isEqualTo(3);
        assertThat(merged.fragmentCount()).isEqualTo(2);
        assertThat(merged.firstIngestedAt()).isEqualTo(INGESTED);
        assertThat(merged.lastIngestedAt()).isEqualTo(INGESTED_LATER);
    }

    @Test
    @DisplayName("of two root spans the one that started first is the root")
    void given_twoRoots_when_merged_then_earlierStartWins() {
        Span early = rootSpan("ffffffffffffffff", "2026-11-21T09:00:00Z");
        Span late = rootSpan("0000000000000001", "2026-11-21T09:00:01Z");

        assertRootOfBothMergeOrders(early, late, early);
    }

    @Test
    @DisplayName("of two root spans with the same start the one with the smaller unsigned id is the root")
    void given_twoRootsWithSameStart_when_merged_then_smallerUnsignedIdWins() {
        Span negativeWhenSigned = rootSpan("8000000000000001", "2026-11-21T09:00:00Z");
        Span small = rootSpan("0000000000000002", "2026-11-21T09:00:00Z");

        assertRootOfBothMergeOrders(negativeWhenSigned, small, small);
    }

    @Test
    @DisplayName("a trace crossing midnight belongs to the day its earliest span started")
    void given_childAfterMidnightArrivesFirst_when_merged_then_dayOfRoot() {
        List<Span> spans = TimeEdgeCases.midnightTrace();
        TraceSummary child = summaryOf(List.of(spans.get(1)), INGESTED);
        TraceSummary root = summaryOf(List.of(spans.get(0)), INGESTED_LATER);

        assertThat(child.merge(root).traceDay()).isEqualTo(new TraceDay(LocalDate.of(2026, 11, 21)));
    }

    @Test
    @DisplayName("rejects summaries of no spans, of mixed traces and merges of different traces")
    void given_wrongInput_when_summarizedOrMerged_then_invalidTraceSummary() {
        Span t1 = IdpScenario.spans(IdpScenario.T1).getFirst();
        Span t2 = IdpScenario.spans(IdpScenario.T2).getFirst();

        assertViolation(ViolationCode.INVALID_TRACE_SUMMARY, () -> summaryOf(List.of(), INGESTED));
        assertViolation(ViolationCode.INVALID_TRACE_SUMMARY, () -> summaryOf(List.of(t1, t2), INGESTED));
        assertViolation(
                ViolationCode.INVALID_TRACE_SUMMARY,
                () -> summaryOf(List.of(t1), INGESTED).merge(summaryOf(List.of(t2), INGESTED)));
    }

    static Stream<Arguments> brokenSummaries() {
        return Stream.of(
                Arguments.of("no spans", aSummary().spanCount(0)),
                Arguments.of("negative errors", aSummary().errorCount(-1)),
                Arguments.of("more errors than spans", aSummary().spanCount(1).errorCount(2)),
                Arguments.of("end before start", aSummary().bounds(2_000, 1_000)),
                Arguments.of("no fragments", aSummary().fragmentCount(0)),
                Arguments.of("last insert before first", aSummary().ingested(INGESTED_LATER, INGESTED)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("brokenSummaries")
    @DisplayName("rejects inconsistent counters and bounds")
    void given_inconsistentValues_when_constructed_then_invalidTraceSummary(String name, TraceSummaryBuilder summary) {
        assertViolation(ViolationCode.INVALID_TRACE_SUMMARY, summary::build);
    }

    private static TraceSummary summaryOf(List<Span> spans, Instant ingestedAt) {
        TraceDay day = spans.isEmpty()
                ? null
                : TraceDay.ofEpochNanos(
                        spans.stream().mapToLong(Span::startEpochNanos).min().orElseThrow());
        return TraceSummary.ofInsertedSpans(
                day,
                spans,
                span -> SERVICE_IDS.getOrDefault(span.serviceName(), OTHER_SERVICE_ID),
                span -> NAME_IDS.get(span.name()),
                ingestedAt);
    }

    private static TraceSummary mergeFromLeft(List<TraceSummary> summaries) {
        return summaries.stream().reduce(TraceSummary::merge).orElseThrow();
    }

    private static TraceSummary mergeFromRight(List<TraceSummary> summaries) {
        TraceSummary result = summaries.getLast();
        for (int i = summaries.size() - 2; i >= 0; i--) {
            result = summaries.get(i).merge(result);
        }
        return result;
    }

    private static Span rootSpan(String spanIdHex, String start) {
        return aSpan().spanId(spanIdHex)
                .service(GATEWAY)
                .start(Instant.parse(start))
                .build();
    }

    private static void assertRootOfBothMergeOrders(Span a, Span b, Span expectedRoot) {
        TraceSummary first = summaryOf(List.of(a), INGESTED);
        TraceSummary second = summaryOf(List.of(b), INGESTED);

        assertThat(first.merge(second).root().spanId()).isEqualTo(expectedRoot.spanId());
        assertThat(second.merge(first).root().spanId()).isEqualTo(expectedRoot.spanId());
    }

    @Test
    @DisplayName("""
            a summary keeps an immutable sorted copy of its service ids:
            changing the set it was built from does not change it""")
    void given_mutableServiceIds_when_summaryBuilt_then_immutableSortedCopy() {
        TraceSummary template = aSummary().build();
        SortedSet<Integer> serviceIds = new TreeSet<>(List.of(3, 1));

        TraceSummary summary = new TraceSummary(
                template.traceId(),
                template.traceDay(),
                template.startEpochNanos(),
                template.endEpochNanos(),
                null,
                1,
                0,
                serviceIds,
                1,
                INGESTED,
                INGESTED);
        serviceIds.add(2);

        assertThat(summary.serviceIds()).containsExactly(1, 3);
        assertThatThrownBy(() -> summary.serviceIds().add(4)).isInstanceOf(UnsupportedOperationException.class);
    }
}
