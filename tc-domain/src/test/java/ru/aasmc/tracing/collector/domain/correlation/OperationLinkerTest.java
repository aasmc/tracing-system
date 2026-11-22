package ru.aasmc.tracing.collector.domain.correlation;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.aasmc.tracing.collector.testmodel.domain.BusinessOperationBuilder.anOperation;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.CODE;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.CODE_KEY_ID;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.SESSION;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.SESSION_KEY_ID;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.T1_OBSERVED;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.T3_OBSERVED;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.trace.TraceId;
import ru.aasmc.tracing.collector.testmodel.domain.ArrivalOrders;
import ru.aasmc.tracing.collector.testmodel.domain.ArrivalOrders.ArrivalOrder;
import ru.aasmc.tracing.collector.testmodel.domain.DomainIds;
import ru.aasmc.tracing.collector.testmodel.domain.IdpScenario;

/**
 * Specification of {@link OperationLinker}, by use case.
 *
 * <p>All tests use the business operation definition {@code idp.login} of {@link IdpScenario}: key {@code idp.auth_session_id} is
 * PRIMARY (its values, written {@code s-…} here, can name an operation), key {@code oauth.authorization_code} is ALIAS
 * (its values, written {@code c-…}, only link), and the merge window is one day. Times are on 2026-11-21. Each test
 * describes the storage state the run starts from (the operations values currently point to, and earlier
 * observations of the same traces), the new observations, and the exact {@link LinkingResult}.
 */
class OperationLinkerTest {

    private static final BusinessOperationDefinition IDP_LOGIN = IdpScenario.operationDefinition();
    private static final Duration WINDOW = IDP_LOGIN.mergeWindow();
    private static final OperationMergePolicy NO_SIZE_LIMIT = new OperationMergePolicy(Integer.MAX_VALUE);
    private static final Instant NEW_IDS_TIMESTAMP = Instant.parse("2026-11-21T12:00:00Z");
    private static final Instant STORED_IDS_TIMESTAMP = Instant.parse("2026-11-20T00:00:00Z");

    private static final TraceId TRACE_1 = trace(1);
    private static final TraceId TRACE_2 = trace(2);
    private static final TraceId TRACE_3 = trace(3);
    private static final TraceId TRACE_4 = trace(4);
    private static final TraceId TRACE_9 = trace(9);

    @Nested
    @DisplayName("Creating operations: values that point to no operation yet")
    class CreatingOperations {

        @Test
        @DisplayName("""
                Given nothing is stored,
                when trace 1 carries the PRIMARY value s-1 at 09:00,
                then one operation is created: named s-1, seen 09:00..09:00, holding 1 trace,
                and s-1 points to it""")
        void given_nothingStored_when_onePrimaryValue_then_namedOperationCreated() {
            LinkingResult result = link(List.of(session(TRACE_1, "s-1", "09:00")));

            assertThat(result)
                    .isEqualTo(expected()
                            .created(operation(newId(1), "09:00", "09:00", "s-1", 1))
                            .pointing("s-1", newId(1))
                            .build());
        }

        @Test
        @DisplayName("""
                Given nothing is stored,
                when trace 1 carries only the ALIAS value c-1,
                then the created operation is provisional: it has no name until a PRIMARY value joins it""")
        void given_nothingStored_when_onlyAliasValue_then_provisionalOperationCreated() {
            LinkingResult result = link(List.of(code(TRACE_1, "c-1", "09:00")));

            assertThat(result)
                    .isEqualTo(expected()
                            .created(operation(newId(1), "09:00", "09:00", null, 1))
                            .pointing("c-1", newId(1))
                            .build());
        }

        @Test
        @DisplayName("""
                Given nothing is stored,
                when one trace carries two values, c-1 and s-1,
                then both belong to one operation, named after the PRIMARY value s-1, holding 1 trace""")
        void given_nothingStored_when_oneTraceCarriesTwoValues_then_oneOperation() {
            LinkingResult result = link(List.of(code(TRACE_1, "c-1", "09:00"), session(TRACE_1, "s-1", "09:00")));

            assertThat(result)
                    .isEqualTo(expected()
                            .created(operation(newId(1), "09:00", "09:00", "s-1", 1))
                            .pointing("c-1", newId(1))
                            .pointing("s-1", newId(1))
                            .build());
        }

        @Test
        @DisplayName("""
                Given nothing is stored,
                when trace 1 carries s-1 (09:00), trace 2 carries s-1 and c-1 (09:05), trace 3 carries c-1 (09:10),
                then all three traces form one operation although traces 1 and 3 share no value:
                trace 2 ties s-1 and c-1 together; the operation is seen 09:00..09:10 and holds 3 traces""")
        void given_nothingStored_when_valuesChainedThroughTraces_then_oneOperation() {
            LinkingResult result = link(List.of(
                    session(TRACE_1, "s-1", "09:00"),
                    session(TRACE_2, "s-1", "09:05"),
                    code(TRACE_2, "c-1", "09:05"),
                    code(TRACE_3, "c-1", "09:10")));

            assertThat(result)
                    .isEqualTo(expected()
                            .created(operation(newId(1), "09:00", "09:10", "s-1", 3))
                            .pointing("s-1", newId(1))
                            .pointing("c-1", newId(1))
                            .build());
        }

        @Test
        @DisplayName("""
                Given nothing is stored,
                when trace 1 carries s-1 and trace 2 carries s-2, and no trace carries both,
                then two independent operations are created, one per value""")
        void given_nothingStored_when_unrelatedValues_then_twoOperations() {
            LinkingResult result = link(List.of(session(TRACE_1, "s-1", "09:00"), session(TRACE_2, "s-2", "09:05")));

            assertThat(result.created()).hasSize(2);
            assertThat(result.created())
                    .extracting(BusinessOperation::displayValue)
                    .containsExactlyInAnyOrder("s-1", "s-2");
            assertThat(result.created())
                    .allSatisfy(operation -> assertThat(operation.traceCount()).isEqualTo(1));
            assertThat(result.operationByValue()).containsOnlyKeys("s-1", "s-2");
            assertThat(result.operationByValue().get("s-1"))
                    .isNotEqualTo(result.operationByValue().get("s-2"));
        }

        @Test
        @DisplayName("""
                Given nothing is stored,
                when the traces of one operation carry several PRIMARY values,
                then the operation is named after the earliest one (s-1 at 09:00 beats s-2 at 09:05),
                and of values seen at the same time after the alphabetically smaller one""")
        void given_severalPrimaryValues_when_created_then_earliestNamesOperation() {
            LinkingResult earliest = link(List.of(
                    session(TRACE_1, "s-2", "09:05"),
                    code(TRACE_1, "c-1", "09:05"),
                    session(TRACE_2, "s-1", "09:00"),
                    code(TRACE_2, "c-1", "09:00")));
            LinkingResult sameTime = link(List.of(session(TRACE_1, "s-b", "09:00"), session(TRACE_1, "s-a", "09:00")));

            assertThat(earliest.created())
                    .singleElement()
                    .extracting(BusinessOperation::displayValue)
                    .isEqualTo("s-1");
            assertThat(sameTime.created())
                    .singleElement()
                    .extracting(BusinessOperation::displayValue)
                    .isEqualTo("s-a");
        }

        @Test
        @DisplayName("""
                Given nothing is stored,
                when the batch contains the same observation twice and s-1 also on a second trace,
                then the operation holds 2 traces: a trace is counted once however often it is observed""")
        void given_duplicateObservations_when_created_then_eachTraceCountedOnce() {
            LinkingResult result = link(List.of(
                    session(TRACE_1, "s-1", "09:00"),
                    session(TRACE_1, "s-1", "09:00"),
                    session(TRACE_2, "s-1", "09:01")));

            assertThat(result.created())
                    .singleElement()
                    .extracting(BusinessOperation::traceCount)
                    .isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("Extending an operation: values that point to one operation")
    class ExtendingAnOperation {

        private final BusinessOperation login = operation(storedId(1), "09:00", "09:00", "s-1", 1);

        @Test
        @DisplayName("""
                Given s-1 points to operation O1 (seen 09:00..09:00, 1 trace),
                when a new trace 2 carries s-1 at 10:00,
                then O1 is updated to seen 09:00..10:00 with 2 traces, and no value is re-pointed""")
        void given_storedOperation_when_newTraceCarriesItsValue_then_operationExtended() {
            LinkingResult result = link(List.of(session(TRACE_2, "s-1", "10:00")), List.of(), Map.of("s-1", login));

            assertThat(result)
                    .isEqualTo(expected()
                            .updated(operation(storedId(1), "09:00", "10:00", "s-1", 2))
                            .build());
        }

        @Test
        @DisplayName("""
                Given s-1 points to O1,
                when trace 2 carries s-1 and the new value c-1,
                then c-1 joins O1: it is re-pointed to O1, and O1 holds 2 traces""")
        void given_storedOperation_when_newValueOnSameTrace_then_newValueJoins() {
            LinkingResult result = link(
                    List.of(session(TRACE_2, "s-1", "10:00"), code(TRACE_2, "c-1", "10:00")),
                    List.of(),
                    Map.of("s-1", login));

            assertThat(result)
                    .isEqualTo(expected()
                            .updated(operation(storedId(1), "09:00", "10:00", "s-1", 2))
                            .pointing("c-1", storedId(1))
                            .build());
        }

        @Test
        @DisplayName("""
                Given s-1 points to O1 (seen from 09:00),
                when s-1 is observed at 08:00, i.e. earlier than O1 was first seen,
                then O1 is widened backwards to seen 08:00..09:00""")
        void given_storedOperation_when_observedEarlier_then_firstSeenWidened() {
            LinkingResult result = link(List.of(session(TRACE_2, "s-1", "08:00")), List.of(), Map.of("s-1", login));

            assertThat(result.updated()).containsExactly(operation(storedId(1), "08:00", "09:00", "s-1", 2));
        }

        @Test
        @DisplayName("""
                Given c-1 points to the provisional operation O2 (no name),
                when trace 2 carries c-1 and the PRIMARY value s-2,
                then O2 is named s-2 and s-2 is re-pointed to it""")
        void given_provisionalOperation_when_primaryValueJoins_then_operationNamed() {
            BusinessOperation provisional = operation(storedId(2), "09:00", "09:00", null, 1);

            LinkingResult result = link(
                    List.of(code(TRACE_2, "c-1", "09:05"), session(TRACE_2, "s-2", "09:05")),
                    List.of(),
                    Map.of("c-1", provisional));

            assertThat(result)
                    .isEqualTo(expected()
                            .updated(operation(storedId(2), "09:00", "09:05", "s-2", 2))
                            .pointing("s-2", storedId(2))
                            .build());
        }

        @Test
        @DisplayName("""
                Given c-1 points to O1, already named s-1,
                when trace 2 carries c-1 and another PRIMARY value s-2,
                then O1 keeps its name s-1: an operation is named once; s-2 still joins O1""")
        void given_namedOperation_when_anotherPrimaryValueJoins_then_nameKept() {
            LinkingResult result = link(
                    List.of(code(TRACE_2, "c-1", "09:05"), session(TRACE_2, "s-2", "09:05")),
                    List.of(),
                    Map.of("c-1", login));

            assertThat(result.updated()).containsExactly(operation(storedId(1), "09:00", "09:05", "s-1", 2));
            assertThat(result.operationByValue()).isEqualTo(Map.of("s-2", storedId(1)));
        }

        @Test
        @DisplayName("""
                Given s-1 points to O1, and an earlier run already linked trace 1 carrying s-1,
                when the same trace 1 now carries the new value c-1 (e.g. the code arrived in a later fragment),
                then c-1 joins O1 through the earlier observation of trace 1,
                and O1 still holds 1 trace, because trace 1 is already counted""")
        void given_earlierObservationOfSameTrace_when_newValueOnThatTrace_then_joinsWithoutRecount() {
            LinkingResult result = link(
                    List.of(code(TRACE_1, "c-1", "09:02")),
                    List.of(session(TRACE_1, "s-1", "09:00")),
                    Map.of("s-1", login));

            assertThat(result)
                    .isEqualTo(expected()
                            .updated(operation(storedId(1), "09:00", "09:02", "s-1", 1))
                            .pointing("c-1", storedId(1))
                            .build());
        }

        @Test
        @DisplayName("""
                Given s-1 and c-1 both point to O1 (2 traces),
                when trace 3 carries s-1 and trace 4 carries c-1, and no trace carries both,
                then both still extend the same O1, which is updated once with 4 traces""")
        void given_twoValuesOfOneOperation_when_seenOnDifferentTraces_then_oneUpdate() {
            BusinessOperation twoTraces = operation(storedId(1), "09:00", "09:00", "s-1", 2);

            LinkingResult result = link(
                    List.of(session(TRACE_3, "s-1", "10:00"), code(TRACE_4, "c-1", "10:05")),
                    List.of(),
                    Map.of("s-1", twoTraces, "c-1", twoTraces));

            assertThat(result)
                    .isEqualTo(expected()
                            .updated(operation(storedId(1), "09:00", "10:05", "s-1", 4))
                            .build());
        }

        @Test
        @DisplayName("""
                Given s-1 points to O1 holding 1 trace, and the size limit is 1 trace,
                when a new trace carries s-1,
                then O1 grows to 2 traces: the size limit guards merges only, never the growth of one operation""")
        void given_sizeLimit_when_singleOperationGrows_then_notLimited() {
            LinkingResult result = link(
                    new OperationMergePolicy(1),
                    List.of(session(TRACE_2, "s-1", "10:00")),
                    List.of(),
                    Map.of("s-1", login));

            assertThat(result.updated()).containsExactly(operation(storedId(1), "09:00", "10:00", "s-1", 2));
            assertThat(result.refusedMerges()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Merge window: a value joins its operation only near the operation in time")
    class MergeWindow {

        private final BusinessOperation login = operation(storedId(1), "09:00", "09:00", "s-1", 1);

        @Test
        @DisplayName("""
                Given s-1 points to O1, last seen at 09:00 on 2026-11-21, merge window one day,
                when s-1 is observed exactly one day later,
                then s-1 still joins O1: the window bound is inclusive""")
        void given_storedOperation_when_observedAtWindowEnd_then_joins() {
            Instant windowEnd = at("09:00").plus(WINDOW);

            LinkingResult result = link(
                    List.of(observation(TRACE_2, SESSION_KEY_ID, "s-1", windowEnd)), List.of(), Map.of("s-1", login));

            assertThat(result.updated())
                    .containsExactly(
                            new BusinessOperation(storedId(1), at("09:00"), windowEnd, IDP_LOGIN.id(), "s-1", 2, null));
        }

        @Test
        @DisplayName("""
                Given s-1 points to O1, last seen at 09:00 on 2026-11-21, merge window one day,
                when s-1 is observed one day and one nanosecond later,
                then s-1 is treated as a reused value: a new operation is created for it, s-1 is re-pointed there,
                and O1 is left unchanged""")
        void given_storedOperation_when_observedAfterWindow_then_newOperation() {
            Instant afterWindow = at("09:00").plus(WINDOW).plusNanos(1);

            LinkingResult result = link(
                    List.of(observation(TRACE_2, SESSION_KEY_ID, "s-1", afterWindow)), List.of(), Map.of("s-1", login));

            assertThat(result)
                    .isEqualTo(expected()
                            .created(new BusinessOperation(
                                    newId(1), afterWindow, afterWindow, IDP_LOGIN.id(), "s-1", 1, null))
                            .pointing("s-1", newId(1))
                            .build());
        }

        @Test
        @DisplayName("""
                Given s-1 points to O1, first seen at 09:00 on 2026-11-21, merge window one day,
                when s-1 is observed exactly one day earlier, or one nanosecond before that,
                then the first joins O1 and widens it backwards, the second starts a new operation""")
        void given_storedOperation_when_observedBeforeFirstSeen_then_windowAppliesBackwards() {
            Instant windowStart = at("09:00").minus(WINDOW);

            LinkingResult atStart = link(
                    List.of(observation(TRACE_2, SESSION_KEY_ID, "s-1", windowStart)), List.of(), Map.of("s-1", login));
            LinkingResult beforeStart = link(
                    List.of(observation(TRACE_2, SESSION_KEY_ID, "s-1", windowStart.minusNanos(1))),
                    List.of(),
                    Map.of("s-1", login));

            assertThat(atStart.updated())
                    .containsExactly(new BusinessOperation(
                            storedId(1), windowStart, at("09:00"), IDP_LOGIN.id(), "s-1", 2, null));
            assertThat(beforeStart.created()).hasSize(1);
            assertThat(beforeStart.updated()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Merging: a trace whose values point to several operations")
    class Merging {

        @Test
        @DisplayName("""
                Given s-1 points to O1 (named s-1, seen 09:05, 1 trace)
                and c-1 points to the provisional O2 (seen 09:00, 1 trace),
                when trace 3 carries both s-1 and c-1 at 09:10,
                then O1 survives although O2 was seen first, because a named operation beats a provisional one;
                O2 is marked as merged into O1, O1 covers both (09:00..09:10, 1 + 1 + 1 = 3 traces),
                and c-1 is re-pointed to O1""")
        void given_namedAndProvisionalOperations_when_bridged_then_mergedIntoNamed() {
            BusinessOperation named = operation(storedId(2), "09:05", "09:05", "s-1", 1);
            BusinessOperation provisional = operation(storedId(1), "09:00", "09:00", null, 1);

            LinkingResult result = link(
                    List.of(session(TRACE_3, "s-1", "09:10"), code(TRACE_3, "c-1", "09:10")),
                    List.of(),
                    Map.of("s-1", named, "c-1", provisional));

            assertThat(result)
                    .isEqualTo(expected()
                            .updated(operation(storedId(2), "09:00", "09:10", "s-1", 3))
                            .merged(provisional.markMergedInto(storedId(2)))
                            .pointing("c-1", storedId(2))
                            .build());
        }

        @Test
        @DisplayName("""
                Given s-1 points to O1 (named, first seen 09:00) and s-3 points to O3 (named, first seen 09:05),
                when one trace carries both s-1 and s-3,
                then O1 survives, because of two named operations the one first seen earlier wins""")
        void given_twoNamedOperations_when_bridged_then_earlierSurvives() {
            BusinessOperation earlier = operation(storedId(2), "09:00", "09:00", "s-1", 1);
            BusinessOperation later = operation(storedId(1), "09:05", "09:05", "s-3", 1);

            LinkingResult result = link(
                    List.of(session(TRACE_9, "s-1", "09:10"), session(TRACE_9, "s-3", "09:10")),
                    List.of(),
                    Map.of("s-1", earlier, "s-3", later));

            assertThat(result.merged()).containsExactly(later.markMergedInto(storedId(2)));
            assertThat(result.operationByValue()).isEqualTo(Map.of("s-3", storedId(2)));
        }

        @Test
        @DisplayName("""
                Given two named operations first seen at the same time,
                when one trace connects them,
                then the operation with the smaller id survives (ids compared as PostgreSQL orders uuid)""")
        void given_sameFirstSeen_when_bridged_then_smallerIdSurvives() {
            BusinessOperation smaller = operation(storedId(1), "09:00", "09:00", "s-1", 1);
            BusinessOperation larger = operation(storedId(2), "09:00", "09:00", "s-2", 1);

            LinkingResult result = link(
                    List.of(session(TRACE_9, "s-2", "09:10"), session(TRACE_9, "s-1", "09:10")),
                    List.of(),
                    Map.of("s-2", larger, "s-1", smaller));

            assertThat(result.merged()).containsExactly(larger.markMergedInto(storedId(1)));
        }

        @Test
        @DisplayName("""
                Given s-1 → O1 (named, 09:00), c-2 → O2 (provisional, 09:02), s-3 → O3 (named, 09:04), 1 trace each,
                when one trace carries s-1, c-2 and s-3 at 09:10,
                then all three become one: O1 survives, O2 and O3 are merged into it,
                O1 holds 1 + 1 + 1 + 1 = 4 traces, and c-2 and s-3 are re-pointed to O1""")
        void given_threeOperations_when_bridged_then_allMergedIntoOneSurvivor() {
            BusinessOperation first = operation(storedId(1), "09:00", "09:00", "s-1", 1);
            BusinessOperation provisional = operation(storedId(2), "09:02", "09:02", null, 1);
            BusinessOperation third = operation(storedId(3), "09:04", "09:04", "s-3", 1);

            LinkingResult result = link(
                    List.of(
                            session(TRACE_9, "s-1", "09:10"),
                            code(TRACE_9, "c-2", "09:10"),
                            session(TRACE_9, "s-3", "09:10")),
                    List.of(),
                    Map.of("s-1", first, "c-2", provisional, "s-3", third));

            assertThat(result.updated()).containsExactly(operation(storedId(1), "09:00", "09:10", "s-1", 4));
            assertThat(result.merged())
                    .containsExactlyInAnyOrder(
                            provisional.markMergedInto(storedId(1)), third.markMergedInto(storedId(1)));
            assertThat(result.operationByValue()).isEqualTo(Map.of("c-2", storedId(1), "s-3", storedId(1)));
            assertThat(result.created()).isEmpty();
        }

        @Test
        @DisplayName("""
                Given a merge of a named and a provisional operation,
                when the new observations come in any order,
                then the decision is the same: it never depends on the order of the input""")
        void given_merge_when_observationsShuffled_then_sameResult() {
            BusinessOperation named = operation(storedId(2), "09:05", "09:05", "s-1", 1);
            BusinessOperation provisional = operation(storedId(1), "09:00", "09:00", null, 1);
            List<CorrelationObservation> observations = new ArrayList<>(List.of(
                    session(TRACE_3, "s-1", "09:10"),
                    code(TRACE_3, "c-1", "09:10"),
                    code(TRACE_4, "c-1", "09:12"),
                    session(TRACE_4, "s-9", "09:12")));
            Map<String, BusinessOperation> current = Map.of("s-1", named, "c-1", provisional);
            LinkingResult inOrder = link(observations, List.of(), current);

            for (long seed = 1; seed <= 5; seed++) {
                Collections.shuffle(observations, new Random(seed));
                assertThat(link(observations, List.of(), current)).isEqualTo(inOrder);
            }
        }
    }

    @Nested
    @DisplayName("Reused values: a value seen again long after its operation")
    class ReusedValues {

        /** s-1 was reused: first by O1 on 2026-11-21, now by O3, seen three days later. */
        private final BusinessOperation reuser = new BusinessOperation(
                storedId(3),
                at("09:00").plus(Duration.ofDays(3)),
                at("09:00").plus(Duration.ofDays(3)),
                IDP_LOGIN.id(),
                "s-1",
                1,
                null);

        @Test
        @DisplayName("""
                Given trace 1 carried s-1 at 09:00 and was linked to O1, and s-1 was reused three days later by O3,
                so s-1 now points to O3,
                when a late fragment of trace 1 brings the new value c-9 at 09:20,
                then s-1 stays with O3: the old trace carried it before O3 reused it, so it is left out;
                c-9 alone starts a provisional operation holding trace 1, and O3 is not touched""")
        void given_valueReusedSince_when_lateObservationOfOldTrace_then_valueNotTakenFromItsCurrentOperation() {
            LinkingResult result = link(
                    List.of(code(TRACE_1, "c-9", "09:20")),
                    List.of(session(TRACE_1, "s-1", "09:00")),
                    Map.of("s-1", reuser));

            assertThat(result)
                    .isEqualTo(expected()
                            .created(operation(newId(1), "09:20", "09:20", null, 1))
                            .pointing("c-9", newId(1))
                            .build());
        }

        @Test
        @DisplayName("""
                Given trace 1 carried s-1 at 09:00, and s-1 now points to O3, which reused it three days later,
                when trace 1's observation is linked again (a replay),
                then nothing changes: a replay never takes a value away from the operation that reused it""")
        void given_valueReusedSince_when_oldTraceReplayed_then_nothingChanges() {
            CorrelationObservation observation = session(TRACE_1, "s-1", "09:00");

            LinkingResult result = link(List.of(observation), List.of(observation), Map.of("s-1", reuser));

            assertThat(result).isEqualTo(expected().build());
        }
    }

    @Nested
    @DisplayName("One run is like several: the traces of a batch are linked one at a time, in time order")
    class TracesOfOneRun {

        private final Instant threeDaysLater = at("09:00").plus(Duration.ofDays(3));

        @Test
        @DisplayName("""
                Given nothing is stored,
                when one batch holds trace 1 carrying s-1 at 09:00 and trace 2 carrying s-1 three days later,
                then they become two operations, exactly as if they had come in two runs:
                trace 2 lies outside the merge window of trace 1's operation, so s-1 counts as reused,
                and s-1 ends pointing to the later operation""")
        void given_sameValueFarApartInOneBatch_when_linked_then_twoOperations() {
            LinkingResult result = link(List.of(
                    observation(TRACE_2, SESSION_KEY_ID, "s-1", threeDaysLater), session(TRACE_1, "s-1", "09:00")));

            assertThat(result)
                    .isEqualTo(expected()
                            .created(operation(newId(1), "09:00", "09:00", "s-1", 1))
                            .created(new BusinessOperation(
                                    newId(2), threeDaysLater, threeDaysLater, IDP_LOGIN.id(), "s-1", 1, null))
                            .pointing("s-1", newId(2))
                            .build());
        }

        @Test
        @DisplayName("""
                Given s-1 points to O1 (seen 09:00, 1 trace),
                when one batch holds trace 2 carrying s-1 at 10:00 and trace 3 carrying s-1 three days later,
                then only trace 2 joins O1 (09:00..10:00, 2 traces),
                and trace 3 starts a new operation that s-1 is re-pointed to""")
        void given_storedOperation_when_batchHasObservationsInsideAndOutsideWindow_then_onlyInsideJoins() {
            BusinessOperation login = operation(storedId(1), "09:00", "09:00", "s-1", 1);

            LinkingResult result = link(
                    List.of(
                            session(TRACE_2, "s-1", "10:00"),
                            observation(TRACE_3, SESSION_KEY_ID, "s-1", threeDaysLater)),
                    List.of(),
                    Map.of("s-1", login));

            assertThat(result)
                    .isEqualTo(expected()
                            .created(new BusinessOperation(
                                    newId(1), threeDaysLater, threeDaysLater, IDP_LOGIN.id(), "s-1", 1, null))
                            .updated(operation(storedId(1), "09:00", "10:00", "s-1", 2))
                            .pointing("s-1", newId(1))
                            .build());
        }

        @Test
        @DisplayName("""
                Given nothing is stored,
                when one batch holds trace 1 carrying c-1 (09:00), trace 2 carrying s-1 (09:01),
                and trace 3 carrying both (09:02),
                then traces 1 and 2 first create two operations and trace 3 merges them;
                because both were created in this run, only the survivor (named s-1) is reported, holding 3 traces,
                and nothing is reported as merged: an operation created and merged in one run never reaches storage""")
        void given_operationsCreatedInThisRun_when_bridgedInSameRun_then_onlySurvivorCreated() {
            LinkingResult result = link(List.of(
                    code(TRACE_1, "c-1", "09:00"),
                    session(TRACE_2, "s-1", "09:01"),
                    session(TRACE_3, "s-1", "09:02"),
                    code(TRACE_3, "c-1", "09:02")));

            assertThat(result)
                    .isEqualTo(expected()
                            .created(operation(newId(2), "09:00", "09:02", "s-1", 3))
                            .pointing("c-1", newId(2))
                            .pointing("s-1", newId(2))
                            .build());
        }

        @Test
        @DisplayName("""
                Given O1 (named s-1, seen 08:00) and the provisional O2 (c-2, seen 09:00) are stored, 1 trace each,
                when one batch holds trace 1 carrying the new s-9 (09:01), trace 2 carrying s-9 and c-2 (09:02),
                and trace 3 carrying s-9 and s-1 (09:03),
                then trace 1 creates an operation X, trace 2 merges O2 into X, and trace 3 merges X into O1;
                O2 is reported as merged straight into O1, X is never reported,
                and O1 ends seen 08:00..09:03 holding 1 + (1 + 1 + 1) + 1 = 5 traces""")
        void given_storedOperationMergedIntoNewOne_when_newOneMergedLater_then_mergedIntoFinalSurvivor() {
            BusinessOperation first = operation(storedId(1), "08:00", "08:00", "s-1", 1);
            BusinessOperation provisional = operation(storedId(2), "09:00", "09:00", null, 1);

            LinkingResult result = link(
                    List.of(
                            session(TRACE_1, "s-9", "09:01"),
                            session(TRACE_2, "s-9", "09:02"),
                            code(TRACE_2, "c-2", "09:02"),
                            session(TRACE_3, "s-9", "09:03"),
                            session(TRACE_3, "s-1", "09:03")),
                    List.of(),
                    Map.of("s-1", first, "c-2", provisional));

            assertThat(result)
                    .isEqualTo(expected()
                            .updated(operation(storedId(1), "08:00", "09:03", "s-1", 5))
                            .merged(provisional.markMergedInto(storedId(1)))
                            .pointing("c-2", storedId(1))
                            .pointing("s-9", storedId(1))
                            .build());
        }

        @Test
        @DisplayName("""
                Given several unrelated traces seen at the same time (with values whose hash codes collide),
                when the batch comes in a different order,
                then every operation gets the same id: traces are linked in the order of their earliest observation,
                ties broken by value and then by trace id, never by input order""")
        void given_unrelatedTraces_when_inputReordered_then_sameIds() {
            List<CorrelationObservation> observations = new ArrayList<>(List.of(
                    session(TRACE_1, "Aa", "09:00"),
                    session(TRACE_2, "BB", "09:00"),
                    code(TRACE_3, "c-1", "09:00"),
                    session(TRACE_4, "s-4", "08:59")));
            assertThat("Aa".hashCode()).isEqualTo("BB".hashCode());
            LinkingResult inOrder = link(observations);

            for (long seed = 1; seed <= 5; seed++) {
                Collections.shuffle(observations, new Random(seed));
                assertThat(link(observations)).isEqualTo(inOrder);
            }
            assertThat(inOrder.operationByValue())
                    .isEqualTo(Map.of("s-4", newId(1), "Aa", newId(2), "BB", newId(3), "c-1", newId(4)));
        }
    }

    @Nested
    @DisplayName("Size limit: no merge may produce an operation with more traces than the limit")
    class SizeLimit {

        private final BusinessOperation named = operation(storedId(1), "09:00", "09:00", "s-1", 1);
        private final BusinessOperation provisional = operation(storedId(2), "09:02", "09:02", null, 1);
        private final List<CorrelationObservation> bridge =
                List.of(session(TRACE_9, "s-1", "09:10"), code(TRACE_9, "c-1", "09:10"));
        private final Map<String, BusinessOperation> current = Map.of("s-1", named, "c-1", provisional);

        @Test
        @DisplayName("""
                Given O1 and O2 with 1 trace each and a size limit of 3 traces,
                when a new trace connects them (1 + 1 + 1 = 3 traces),
                then they are merged: a merge exactly at the limit is allowed""")
        void given_mergeAtLimit_when_linked_then_merged() {
            LinkingResult result = link(new OperationMergePolicy(3), bridge, List.of(), current);

            assertThat(result.merged()).containsExactly(provisional.markMergedInto(storedId(1)));
            assertThat(result.refusedMerges()).isEmpty();
        }

        @Test
        @DisplayName("""
                Given O1 and O2 with 1 trace each and a size limit of 2 traces,
                when a new trace connects them (3 traces),
                then the merge is refused and reported with survivor, other operation and trace count;
                nothing is merged and no value is re-pointed, but O1 and O2 each count the trace,
                which carries a value of each of them.
                Such a value is usually shared by unrelated traces (e.g. a constant placeholder)""")
        void given_mergeAboveLimit_when_linked_then_refusedAndTraceCountedByBoth() {
            LinkingResult result = link(new OperationMergePolicy(2), bridge, List.of(), current);

            assertThat(result)
                    .isEqualTo(expected()
                            .updated(provisional.withTraceCount(2))
                            .updated(named.withTraceCount(2))
                            .refused(new RefusedMerge(named, provisional, 3))
                            .build());
        }

        @Test
        @DisplayName("""
                Given three operations with 1 trace each and a size limit of 3,
                when one trace connects all three (4 traces),
                then both merges into the survivor are refused, each reported separately""")
        void given_threeOperationsAboveLimit_when_linked_then_eachMergeRefused() {
            BusinessOperation third = operation(storedId(3), "09:04", "09:04", "s-3", 1);

            LinkingResult result = link(
                    new OperationMergePolicy(3),
                    List.of(
                            session(TRACE_9, "s-1", "09:10"),
                            code(TRACE_9, "c-1", "09:10"),
                            session(TRACE_9, "s-3", "09:10")),
                    List.of(),
                    Map.of("s-1", named, "c-1", provisional, "s-3", third));

            assertThat(result.refusedMerges())
                    .containsExactlyInAnyOrder(
                            new RefusedMerge(named, provisional, 4), new RefusedMerge(named, third, 4));
            assertThat(result.merged()).isEmpty();
            assertThat(result.updated())
                    .containsExactlyInAnyOrder(
                            named.withTraceCount(2), provisional.withTraceCount(2), third.withTraceCount(2));
        }

        @Test
        @DisplayName("""
                Given a size limit of 2 traces and trace 9, which connected O1 and O2 in an earlier run whose merge
                was refused: O1 and O2 hold 2 traces each, trace 9 counted in both,
                and trace 9's own new value c-9 was left without an operation,
                when trace 9 brings another new value c-10,
                then the merge needs 2 + 2 - 2 + 1 = 3 traces (trace 9 once) and is refused again;
                no count changes and c-9 and c-10 stay without an operation""")
        void given_traceCountedByBothAfterRefusal_when_moreValuesArrive_then_refusedAgain() {
            BusinessOperation namedWithTrace9 = named.withTraceCount(2);
            BusinessOperation provisionalWithTrace9 = provisional.withTraceCount(2);
            List<CorrelationObservation> earlier = new ArrayList<>(bridge);
            earlier.add(code(TRACE_9, "c-9", "09:10"));

            LinkingResult result = link(
                    new OperationMergePolicy(2),
                    List.of(code(TRACE_9, "c-10", "09:20")),
                    earlier,
                    Map.of("s-1", namedWithTrace9, "c-1", provisionalWithTrace9));

            assertThat(result)
                    .isEqualTo(expected()
                            .refused(new RefusedMerge(namedWithTrace9, provisionalWithTrace9, 3))
                            .build());
        }
    }

    @Nested
    @DisplayName("Trace counting: how many distinct traces an operation holds after the run")
    class TraceCounting {

        @Test
        @DisplayName("""
                Given O1 (s-1) and O2 (c-1) hold 2 traces each, and trace 2 is in both
                (possible after a refused merge: trace 2 carried s-1 and c-1),
                when trace 9 carries s-1 and c-1 and the merge is allowed,
                then O1 is counted 2 + 2 + 1 = 5 traces, although only 4 distinct traces exist:
                a trace held by two operations is counted twice, so the count is never too low
                and the size limit can never be exceeded unnoticed""")
        void given_traceHeldByTwoOperations_when_merged_then_countedTwice() {
            BusinessOperation sessionOperation = operation(storedId(1), "09:00", "09:01", "s-1", 2);
            BusinessOperation codeOperation = operation(storedId(2), "09:01", "09:02", null, 2);

            LinkingResult result = link(
                    List.of(session(TRACE_9, "s-1", "09:10"), code(TRACE_9, "c-1", "09:10")),
                    List.of(session(TRACE_2, "s-1", "09:01"), code(TRACE_2, "c-1", "09:01")),
                    Map.of("s-1", sessionOperation, "c-1", codeOperation));

            assertThat(result.updated()).containsExactly(operation(storedId(1), "09:00", "09:10", "s-1", 5));
        }
    }

    @Nested
    @DisplayName("Idempotence and statelessness")
    class IdempotenceAndStatelessness {

        @Test
        @DisplayName("""
                Given trace 1 carrying s-1 was linked to O1 in an earlier run,
                when the same observation is linked again (e.g. a replayed Kafka batch),
                then the result is empty: nothing is created, updated, merged or re-pointed""")
        void given_alreadyLinked_when_linkedAgain_then_nothingChanges() {
            BusinessOperation login = operation(storedId(1), "09:00", "09:00", "s-1", 1);
            CorrelationObservation observation = session(TRACE_1, "s-1", "09:00");

            LinkingResult result = link(List.of(observation), List.of(observation), Map.of("s-1", login));

            assertThat(result).isEqualTo(expected().build());
        }

        @Test
        @DisplayName("""
                Given an earlier observation of a trace that has no new observation in this run,
                when the run is linked,
                then that earlier value is left alone: only traces with new observations are linked""")
        void given_groupWithoutNewObservations_when_linked_then_ignored() {
            LinkingResult result =
                    link(List.of(session(TRACE_1, "s-1", "09:00")), List.of(code(TRACE_4, "c-4", "08:00")), Map.of());

            assertThat(result.created())
                    .singleElement()
                    .extracting(BusinessOperation::displayValue)
                    .isEqualTo("s-1");
            assertThat(result.operationByValue()).containsOnlyKeys("s-1");
        }

        @Test
        @DisplayName("""
                Given no new observations,
                when the run is linked,
                then the result is empty""")
        void given_noObservations_when_linked_then_emptyResult() {
            assertThat(link(List.of())).isEqualTo(expected().build());
        }

        @Test
        @DisplayName("""
                Given one linker instance,
                when it links the same input twice without the result being stored in between,
                then it creates the operation both times (with a new id each time):
                the linker remembers nothing between calls; only storage records what was linked""")
        void given_oneLinker_when_sameInputTwice_then_noMemoryBetweenCalls() {
            OperationLinker linker = new OperationLinker(NO_SIZE_LIMIT, DomainIds.uuidV7Sequence(NEW_IDS_TIMESTAMP));
            LinkingInput input =
                    new LinkingInput(IDP_LOGIN, List.of(session(TRACE_1, "s-1", "09:00")), List.of(), Map.of());

            LinkingResult first = linker.link(input);
            LinkingResult second = linker.link(input);

            assertThat(first.created()).containsExactly(operation(newId(1), "09:00", "09:00", "s-1", 1));
            assertThat(second.created()).containsExactly(operation(newId(2), "09:00", "09:00", "s-1", 1));
        }
    }

    @Nested
    @DisplayName("Input rules: a run looks at one business operation definition and at live operations only")
    class InputRules {

        @Test
        @DisplayName("""
                Given an observation of another business operation definition, or of a key the definition does not have,
                when the linking input is built,
                then it is rejected with INVALID_OPERATION_DEFINITION""")
        void given_foreignObservation_when_inputBuilt_then_invalidOperationDefinition() {
            assertViolation(
                    ViolationCode.INVALID_OPERATION_DEFINITION,
                    () -> inputOf(List.of(new CorrelationObservation(
                            at("09:00"), UUID.randomUUID(), SESSION_KEY_ID, "s-1", TRACE_1))));
            assertViolation(
                    ViolationCode.INVALID_OPERATION_DEFINITION,
                    () -> inputOf(List.of(new CorrelationObservation(
                            at("09:00"), IDP_LOGIN.id(), UUID.randomUUID(), "s-1", TRACE_1))));
        }

        @Test
        @DisplayName("""
                Given a current operation that was already merged into another one, or is of another definition,
                when the linking input is built,
                then it is rejected with INVALID_BUSINESS_OPERATION:
                values must point to live operations of the run's definition""")
        void given_mergedOrForeignOperation_when_inputBuilt_then_invalidBusinessOperation() {
            BusinessOperation merged =
                    operation(storedId(1), "09:00", "09:00", "s-1", 1).markMergedInto(storedId(2));
            BusinessOperation otherDefinition =
                    new BusinessOperation(storedId(3), at("09:00"), at("09:00"), UUID.randomUUID(), "s-1", 1, null);
            List<CorrelationObservation> observations = List.of(session(TRACE_1, "s-1", "09:00"));

            assertViolation(
                    ViolationCode.INVALID_BUSINESS_OPERATION,
                    () -> new LinkingInput(IDP_LOGIN, observations, List.of(), Map.of("s-1", merged)));
            assertViolation(
                    ViolationCode.INVALID_BUSINESS_OPERATION,
                    () -> new LinkingInput(IDP_LOGIN, observations, List.of(), Map.of("s-1", otherDefinition)));
        }

        @Test
        @DisplayName("""
                Given two values mapped to the same operation id, but to two different versions of it
                (e.g. read from storage at different moments, with different trace counts),
                when the linking input is built,
                then it is rejected with INVALID_BUSINESS_OPERATION: an operation must look the same through every value""")
        void given_twoVersionsOfOneOperation_when_inputBuilt_then_invalidBusinessOperation() {
            BusinessOperation oneTrace = operation(storedId(1), "09:00", "09:00", "s-1", 1);
            BusinessOperation twoTraces = operation(storedId(1), "09:00", "09:00", "s-1", 2);

            assertViolation(
                    ViolationCode.INVALID_BUSINESS_OPERATION,
                    () -> new LinkingInput(
                            IDP_LOGIN,
                            List.of(session(TRACE_1, "s-1", "09:00"), code(TRACE_1, "c-1", "09:00")),
                            List.of(),
                            Map.of("s-1", oneTrace, "c-1", twoTraces)));
        }

        @Test
        @DisplayName("""
                Given mutable lists and maps,
                when the linking input is built and the originals change afterwards,
                then the input keeps the values it was built with""")
        void given_mutableCollections_when_inputBuilt_then_copiesKept() {
            List<CorrelationObservation> observations = new ArrayList<>(List.of(session(TRACE_1, "s-1", "09:00")));
            Map<String, BusinessOperation> current = new HashMap<>();

            LinkingInput input = new LinkingInput(IDP_LOGIN, observations, List.of(), current);
            observations.clear();
            current.put("s-1", operation(storedId(1), "09:00", "09:00", "s-1", 1));

            assertThat(input.newObservations()).hasSize(1);
            assertThat(input.currentOperationByValue()).isEmpty();
        }

        private static LinkingInput inputOf(List<CorrelationObservation> newObservations) {
            return new LinkingInput(IDP_LOGIN, newObservations, List.of(), Map.of());
        }
    }

    @Nested
    @DisplayName("Across runs: the IdP login linked run after run, with storage in between")
    class AcrossRuns {

        @ParameterizedTest(name = "{0}")
        @MethodSource("ru.aasmc.tracing.collector.testmodel.domain.ArrivalOrders#idpArrivalOrders")
        @DisplayName("""
                Given the login's traces T1 (session), T2 (session and code) and T3 (code),
                when they arrive in any order, one per run, or all in one run,
                then they always end as one operation named as-777, seen from T1 to T3, holding the 3 traces,
                with both values pointing to it""")
        void given_arrivalOrder_when_linkedRunByRun_then_oneOperation(ArrivalOrder order) {
            OperationStore store = new OperationStore(OperationMergePolicy.DEFAULT);

            order.batches().forEach(store::link);

            BusinessOperation login = store.onlyLiveOperation();
            assertThat(login.displayValue()).isEqualTo(SESSION);
            assertThat(login.traceCount()).isEqualTo(3);
            assertThat(login.firstSeen()).isEqualTo(T1_OBSERVED);
            assertThat(login.lastSeen()).isEqualTo(T3_OBSERVED);
            assertThat(store.operationByValue).containsOnlyKeys(SESSION, CODE).containsValue(login.id());
            assertThat(store.tracesOf(login.id()))
                    .containsExactlyInAnyOrder(IdpScenario.T1, IdpScenario.T2, IdpScenario.T3);
        }

        @Test
        @DisplayName("""
                Given the whole login was linked (each observation even twice in the same run),
                when the same observations are linked again in a later run,
                then nothing changes and the operation still holds 3 traces""")
        void given_linkedLogin_when_sameObservationsLinkedAgain_then_noChanges() {
            OperationStore store = new OperationStore(OperationMergePolicy.DEFAULT);
            List<CorrelationObservation> login = Stream.of(IdpScenario.T1, IdpScenario.T2, IdpScenario.T3)
                    .flatMap(trace -> IdpScenario.observations(trace).stream())
                    .toList();

            store.link(Stream.concat(login.stream(), login.stream()).toList());
            LinkingResult replay = store.link(login);

            assertThat(store.onlyLiveOperation().traceCount()).isEqualTo(3);
            assertThat(replay).isEqualTo(expected().build());
        }

        @Test
        @DisplayName("""
                Given the login was linked,
                when session id as-777 is seen again three days later on a new trace (outside the one-day window),
                then a new operation is created for it and the login operation is not touched""")
        void given_sessionIdReusedLater_when_linked_then_newOperation() {
            OperationStore store = new OperationStore(OperationMergePolicy.DEFAULT);
            List<List<CorrelationObservation>> batches =
                    ArrivalOrders.reusedOutsideWindow().batches();

            store.link(batches.get(0));
            LinkingResult reuse = store.link(batches.get(1));

            assertThat(reuse.created()).singleElement().satisfies(operation -> {
                assertThat(operation.displayValue()).isEqualTo(SESSION);
                assertThat(operation.traceCount()).isEqualTo(1);
                assertThat(operation.firstSeen()).isEqualTo(T1_OBSERVED.plus(Duration.ofDays(3)));
            });
            assertThat(reuse.updated()).isEmpty();
            assertThat(reuse.merged()).isEmpty();
        }

        @Test
        @DisplayName("""
                Given the login was linked,
                when a trace of another session as-778 carries the same code,
                then as-778 joins the login operation, which now holds 4 traces""")
        void given_twoSessionsSharingCode_when_linked_then_oneOperation() {
            OperationStore store = new OperationStore(OperationMergePolicy.DEFAULT);

            ArrivalOrders.twoSessionsBridged().batches().forEach(store::link);

            BusinessOperation login = store.onlyLiveOperation();
            assertThat(login.displayValue()).isEqualTo(SESSION);
            assertThat(login.traceCount()).isEqualTo(4);
            assertThat(store.operationByValue)
                    .containsOnlyKeys(SESSION, "as-778", CODE)
                    .containsValue(login.id());
        }

        @Test
        @DisplayName("""
                Given T1 and T3 were linked into two operations in two runs, and the size limit is 2 traces,
                when T2 connects them in a third run (3 traces),
                then the merge is refused and both operations stay""")
        void given_bridgeAboveSizeLimit_when_linked_then_mergeRefused() {
            OperationStore store = new OperationStore(new OperationMergePolicy(2));
            List<List<CorrelationObservation>> batches =
                    ArrivalOrders.oversizedMerge().batches();

            store.link(batches.get(0));
            store.link(batches.get(1));
            LinkingResult bridge = store.link(batches.get(2));

            assertThat(bridge.merged()).isEmpty();
            assertThat(bridge.refusedMerges()).singleElement().satisfies(refused -> {
                assertThat(refused.survivor().displayValue()).isEqualTo(SESSION);
                assertThat(refused.combinedTraceCount()).isEqualTo(3);
            });
            assertThat(store.liveOperations()).hasSize(2);
        }
    }

    private static LinkingResult link(List<CorrelationObservation> newObservations) {
        return link(newObservations, List.of(), Map.of());
    }

    private static LinkingResult link(
            List<CorrelationObservation> newObservations,
            List<CorrelationObservation> earlierObservations,
            Map<String, BusinessOperation> currentOperationByValue) {
        return link(NO_SIZE_LIMIT, newObservations, earlierObservations, currentOperationByValue);
    }

    /** One run with a fresh linker whose new ids are {@code newId(1)}, {@code newId(2)}, … */
    private static LinkingResult link(
            OperationMergePolicy policy,
            List<CorrelationObservation> newObservations,
            List<CorrelationObservation> earlierObservations,
            Map<String, BusinessOperation> currentOperationByValue) {
        OperationLinker linker = new OperationLinker(policy, DomainIds.uuidV7Sequence(NEW_IDS_TIMESTAMP));
        return linker.link(new LinkingInput(IDP_LOGIN, newObservations, earlierObservations, currentOperationByValue));
    }

    private static Instant at(String time) {
        return Instant.parse("2026-11-21T" + time + ":00Z");
    }

    private static TraceId trace(int number) {
        return DomainIds.traceId("%032x".formatted(number));
    }

    /** An observation of the PRIMARY key {@code idp.auth_session_id}. */
    private static CorrelationObservation session(TraceId trace, String value, String time) {
        return observation(trace, SESSION_KEY_ID, value, at(time));
    }

    /** An observation of the ALIAS key {@code oauth.authorization_code}. */
    private static CorrelationObservation code(TraceId trace, String value, String time) {
        return observation(trace, CODE_KEY_ID, value, at(time));
    }

    private static CorrelationObservation observation(TraceId trace, UUID keyId, String value, Instant time) {
        return new CorrelationObservation(time, IDP_LOGIN.id(), keyId, value, trace);
    }

    /** A live operation of {@code idp.login}; {@code displayValue} {@code null} makes it provisional. */
    private static BusinessOperation operation(
            UUID id, String firstSeen, String lastSeen, String displayValue, int traceCount) {
        return anOperation()
                .id(id)
                .seen(at(firstSeen), at(lastSeen))
                .displayValue(displayValue)
                .traceCount(traceCount)
                .build();
    }

    /** The n-th id the linker of {@link #link} hands out to new operations. */
    private static UUID newId(int n) {
        return nthId(NEW_IDS_TIMESTAMP, n);
    }

    /** Ids of stored operations; {@code storedId(1) < storedId(2) < …}, all smaller than new ids. */
    private static UUID storedId(int n) {
        return nthId(STORED_IDS_TIMESTAMP, n);
    }

    private static UUID nthId(Instant timestamp, int n) {
        Supplier<UUID> ids = DomainIds.uuidV7Sequence(timestamp);
        UUID id = ids.get();
        for (int i = 1; i < n; i++) {
            id = ids.get();
        }
        return id;
    }

    private static ExpectedResult expected() {
        return new ExpectedResult();
    }

    /** Builds the expected {@link LinkingResult}; whatever is not mentioned is expected to be empty. */
    private static final class ExpectedResult {

        private final List<BusinessOperation> created = new ArrayList<>();
        private final List<BusinessOperation> updated = new ArrayList<>();
        private final List<BusinessOperation> merged = new ArrayList<>();
        private final Map<String, UUID> operationByValue = new LinkedHashMap<>();
        private final List<RefusedMerge> refused = new ArrayList<>();

        ExpectedResult created(BusinessOperation operation) {
            created.add(operation);
            return this;
        }

        ExpectedResult updated(BusinessOperation operation) {
            updated.add(operation);
            return this;
        }

        ExpectedResult merged(BusinessOperation operation) {
            merged.add(operation);
            return this;
        }

        ExpectedResult pointing(String value, UUID operationId) {
            operationByValue.put(value, operationId);
            return this;
        }

        ExpectedResult refused(RefusedMerge refusal) {
            refused.add(refusal);
            return this;
        }

        LinkingResult build() {
            return new LinkingResult(created, updated, merged, operationByValue, refused);
        }
    }

    /**
     * Plays the role of storage between linker runs: each run gets the new observations, the stored observations of the
     * same traces and the current operation of every value involved; its result is written back like the database
     * would, including pointing the other values of a merged operation to the survivor.
     */
    private static final class OperationStore {

        private final OperationLinker linker;
        private final Map<UUID, BusinessOperation> operations = new HashMap<>();
        private final Map<String, UUID> operationByValue = new HashMap<>();
        private final List<CorrelationObservation> observations = new ArrayList<>();

        OperationStore(OperationMergePolicy policy) {
            this.linker = new OperationLinker(policy, DomainIds.uuidV7Sequence(T1_OBSERVED));
        }

        LinkingResult link(List<CorrelationObservation> batch) {
            Set<TraceId> batchTraces =
                    batch.stream().map(CorrelationObservation::traceId).collect(Collectors.toSet());
            List<CorrelationObservation> earlier = observations.stream()
                    .filter(observation -> batchTraces.contains(observation.traceId()))
                    .toList();
            Map<String, BusinessOperation> currentOperationByValue = new HashMap<>();
            Stream.concat(batch.stream(), earlier.stream())
                    .map(CorrelationObservation::value)
                    .filter(operationByValue::containsKey)
                    .forEach(value -> currentOperationByValue.put(value, operations.get(operationByValue.get(value))));

            LinkingResult result = linker.link(new LinkingInput(IDP_LOGIN, batch, earlier, currentOperationByValue));

            Stream.of(result.created(), result.updated(), result.merged())
                    .flatMap(List::stream)
                    .forEach(operation -> operations.put(operation.id(), operation));
            operationByValue.putAll(result.operationByValue());
            for (BusinessOperation absorbed : result.merged()) {
                operationByValue.replaceAll((value, id) -> id.equals(absorbed.id()) ? absorbed.mergedInto() : id);
            }
            observations.addAll(batch);
            return result;
        }

        List<BusinessOperation> liveOperations() {
            return operations.values().stream()
                    .filter(operation -> operation.mergedInto() == null)
                    .toList();
        }

        BusinessOperation onlyLiveOperation() {
            assertThat(liveOperations()).hasSize(1);
            return liveOperations().getFirst();
        }

        Set<TraceId> tracesOf(UUID operationId) {
            return observations.stream()
                    .filter(observation -> operationId.equals(operationByValue.get(observation.value())))
                    .map(CorrelationObservation::traceId)
                    .collect(Collectors.toSet());
        }
    }
}
