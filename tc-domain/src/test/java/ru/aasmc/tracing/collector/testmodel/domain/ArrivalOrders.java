package ru.aasmc.tracing.collector.testmodel.domain;

import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.CODE;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.CODE_KEY_ID;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.SESSION;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.SESSION_KEY_ID;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.T1;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.T1_OBSERVED;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.T2;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.T3;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.observation;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import ru.aasmc.tracing.collector.domain.correlation.CorrelationObservation;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * Orders in which the correlation observations of the {@link IdpScenario} login can arrive. An arrival order is a list
 * of batches; each batch is what one {@code OperationLinker} run sees. However the observations arrive, they must end
 * as one operation {@code as-777} holding T1, T2 and T3.
 */
public final class ArrivalOrders {

    /** A later login that reuses session id {@code as-777} three days later, outside the one-day merge window. */
    public static final TraceId T4_REUSED = DomainIds.traceId("4444444444444444444444444444d4d4");

    /** A second session {@code as-778} that shares {@code code-abc} with the first. */
    public static final TraceId T5_OTHER_SESSION = DomainIds.traceId("5555555555555555555555555555e5e5");

    private ArrivalOrders() {}

    public record ArrivalOrder(String name, List<List<CorrelationObservation>> batches) {

        @Override
        public String toString() {
            return name;
        }
    }

    /** Every order of the three traces, one trace per batch, plus all three in a single batch. */
    public static List<ArrivalOrder> idpArrivalOrders() {
        List<ArrivalOrder> result = new ArrayList<>();
        for (List<TraceId> order : Permutations.allOrders(List.of(T1, T2, T3))) {
            result.add(new ArrivalOrder(
                    order.stream().map(ArrivalOrders::label).toList().toString(),
                    order.stream().map(IdpScenario::observations).toList()));
        }
        result.add(new ArrivalOrder("all at once", List.of(allIdpObservations())));
        return result;
    }

    /** The IdP login, then {@code as-777} reused on T4: T4 must start a new operation. */
    public static ArrivalOrder reusedOutsideWindow() {
        Instant later = T1_OBSERVED.plus(Duration.ofDays(3));
        return new ArrivalOrder(
                "as-777 reused after 3 days",
                List.of(allIdpObservations(), List.of(observation(T4_REUSED, later, SESSION_KEY_ID, SESSION))));
    }

    /**
     * T1 and T3 arrive first and form two operations; then T2 connects them. With a limit of two traces per
     * operation the merge (three traces) must be refused.
     */
    public static ArrivalOrder oversizedMerge() {
        return new ArrivalOrder(
                "T1, T3, then T2 bridging them",
                List.of(IdpScenario.observations(T1), IdpScenario.observations(T3), IdpScenario.observations(T2)));
    }

    /**
     * The IdP login, then T5 with session {@code as-778} and the same code. Both operations have a display value, so
     * the one seen first ({@code as-777}) survives.
     */
    public static ArrivalOrder twoSessionsBridged() {
        Instant seen = Instant.parse("2026-11-21T09:16:00Z");
        return new ArrivalOrder(
                "as-778 bridged by code-abc",
                List.of(
                        allIdpObservations(),
                        List.of(
                                observation(T5_OTHER_SESSION, seen, SESSION_KEY_ID, "as-778"),
                                observation(T5_OTHER_SESSION, seen, CODE_KEY_ID, CODE))));
    }

    private static List<CorrelationObservation> allIdpObservations() {
        return List.of(T1, T2, T3).stream()
                .flatMap(t -> IdpScenario.observations(t).stream())
                .toList();
    }

    private static String label(TraceId trace) {
        return trace.equals(T1) ? "T1" : trace.equals(T2) ? "T2" : "T3";
    }
}
