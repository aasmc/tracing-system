package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * Groups the traces of one business operation definition into business operations.
 *
 * <p><b>Why.</b> One business action, e.g. a login, often takes several requests, and each request is its own trace.
 * The traces do not reference each other, but they carry shared attribute values. The definition {@code idp.login}
 * names two such attributes: the session id {@code idp.auth_session_id}, a PRIMARY key whose first value becomes the
 * operation's display value, and the authorization code {@code oauth.authorization_code}, an ALIAS key that only
 * links. An <em>observation</em> is the fact "trace T carried value V at time t".
 *
 * <p><b>The rule: two values belong to the same operation if one trace carried both</b>, directly or through a chain of
 * traces. A login whose first request carries {@code as-777}, whose second carries {@code as-777} and
 * {@code code-abc}, and whose third carries {@code code-abc} is one operation, although the first and the third
 * request share no value. Values are matched only within the definition's merge window: a session id seen again days
 * later is a recycled id and starts a new operation.
 *
 * <p><b>Input and output.</b> Storage keeps the operations and, for each value, the operation it belongs to (its
 * <em>mapping</em>, e.g. {@code as-777 → O1}). The linker itself keeps nothing between calls. Every few seconds the
 * caller loads the new observations, the earlier observations of the same traces and the current operation of every
 * value involved ({@link LinkingInput}), calls {@link #link} and writes the {@link LinkingResult} back in one
 * transaction. Two runs for the same definition must not overlap, or both could create an operation for the same new
 * value.
 *
 * <p><b>How a trace is linked.</b> The traces of a batch are linked one at a time, ordered by their earliest new
 * observation; each trace sees what the traces before it changed. For one trace:
 *
 * <ol>
 *   <li>Find the operations its values can join. A value can join the operation it is mapped to if the trace carried
 *       it within that operation's merge window ({@link OperationMergePolicy#canJoin}). A value mapped to an operation
 *       outside the window is treated in one of two ways:
 *       <ul>
 *         <li>if this trace carries it for the first time, the value was recycled: it is treated like a new value;
 *         <li>if this trace already carried it in an earlier run, a newer operation has taken the value over since
 *             then: the value is left out, so a late fragment of an old trace never takes a value away from the
 *             operation that uses it now.
 *       </ul>
 *   <li>No operation found: create one. One: extend it. Several: merge them into the survivor chosen by
 *       {@link OperationMergePolicy#survivor}, and mark the others as merged into it.
 *   <li>Map the trace's values to the resulting operation.
 * </ol>
 *
 * Each operation records its time bounds, its display value and how many distinct traces it holds. An operation that
 * is created and merged within the same run never reaches storage.
 *
 * <p><b>Size limit.</b> A merge that would give an operation more traces than {@link OperationMergePolicy#allowsMerge}
 * allows is not made; it is reported in the result instead. The trace then belongs to each of these operations,
 * because it carries a value of each, so each of them counts it; none of its values is mapped.
 *
 * <p><b>Example</b> (merge window one day, all times on 2026-11-21). The three requests of a login arrive as T1, T3
 * and T2, each in its own run:
 *
 * <ol>
 *   <li>T1 carries {@code as-777} at 09:15:02. The value is new: create O1 (seen 09:15:02..09:15:02, display value
 *       {@code as-777}, 1 trace) and map {@code as-777 → O1}.
 *   <li>T3 carries {@code code-abc} at 09:15:33. The value is new: create O2 with 1 trace and no display value, since an
 *       authorization code is an ALIAS value; map {@code code-abc → O2}.
 *   <li>T2 carries both values at 09:15:31, within a day of O1 and of O2: merge them. O1 survives (an operation with a
 *       display value wins), is widened to 09:15:02..09:15:33 and holds 3 traces; O2 is marked as merged into O1, and
 *       {@code code-abc} is mapped to O1.
 *   <li>The same observations of T2 arrive again (a replay): O1 already counts T2, so the result is empty.
 *   <li>Three days later T4 carries {@code as-777}. O1 is outside the window, so the session id was recycled: create O3
 *       and map {@code as-777 → O3}. O1 does not change.
 *   <li>A late fragment of T1 carries {@code code-xyz}. T1's earlier value {@code as-777} now belongs to O3, three days
 *       away, so it was taken over and is left out; {@code code-xyz} alone creates a new operation. Storage keeps only
 *       the current operation of each value, so the fragment cannot find its way back to O1.
 * </ol>
 *
 * With a size limit of 2, step 3 would need 3 traces: O1 and O2 are not merged, the refusal is reported, and both of
 * them now count T2 (2 traces each).
 *
 * <p>Linking traces in one run or in several runs in the same order gives the same result, and linking the same
 * observations again changes nothing. Safe to share between threads.
 */
public final class OperationLinker {

    /** Earlier observations first; ties by value, then by trace, so the result does not depend on input order. */
    private static final Comparator<CorrelationObservation> IN_TIME_ORDER = Comparator.comparing(
                    CorrelationObservation::observedAt)
            .thenComparing(CorrelationObservation::value)
            .thenComparing(CorrelationObservation::traceId);

    private final OperationMergePolicy policy;
    private final Supplier<UUID> newId;

    /**
     * @param policy survivor, merge-window and size rules
     * @param newId supplies ids for new operations (version-7 UUIDs)
     */
    public OperationLinker(OperationMergePolicy policy, Supplier<UUID> newId) {
        this.policy = policy;
        this.newId = newId;
    }

    /**
     * Links the new observations of {@code input}, as described in the class documentation.
     *
     * @return what storage has to write
     */
    public LinkingResult link(LinkingInput input) {
        Map<TraceId, List<CorrelationObservation>> newByTrace = byTrace(input.newObservations());
        Map<TraceId, List<CorrelationObservation>> earlierByTrace = byTrace(input.earlierObservations());
        List<TraceId> tracesInTimeOrder = input.newObservations().stream()
                .sorted(IN_TIME_ORDER)
                .map(CorrelationObservation::traceId)
                .distinct()
                .toList();
        WorkingCopy workingCopy = new WorkingCopy(input.currentOperationByValue());
        for (TraceId trace : tracesInTimeOrder) {
            linkTrace(
                    newByTrace.get(trace),
                    earlierByTrace.getOrDefault(trace, List.of()),
                    input.definition(),
                    workingCopy);
        }
        return workingCopy.changes();
    }

    /**
     * Links one trace, as described in the class documentation: creates, extends or merges its operation, or reports a
     * refused merge, and maps its values.
     */
    private void linkTrace(
            List<CorrelationObservation> newObservations,
            List<CorrelationObservation> earlierObservations,
            BusinessOperationDefinition definition,
            WorkingCopy workingCopy) {
        List<CorrelationObservation> traceObservations = Stream.concat(
                        newObservations.stream(), earlierObservations.stream())
                .toList();
        Map<String, BusinessOperation> joinableOperationByValue =
                joinableOperationByValue(traceObservations, definition.mergeWindow(), workingCopy);
        Set<String> valuesTakenOverByNewerOperation = earlierObservations.stream()
                .map(CorrelationObservation::value)
                .filter(value -> workingCopy.operationOf(value) != null && !joinableOperationByValue.containsKey(value))
                .collect(Collectors.toSet());

        List<CorrelationObservation> linkedObservations = traceObservations.stream()
                .filter(observation -> !valuesTakenOverByNewerOperation.contains(observation.value()))
                .sorted(IN_TIME_ORDER)
                .toList();
        if (linkedObservations.isEmpty()) {
            return;
        }
        List<BusinessOperation> operations = linkedObservations.stream()
                .map(observation -> joinableOperationByValue.get(observation.value()))
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        Set<UUID> operationsCountingTrace = earlierObservations.stream()
                .map(observation -> joinableOperationByValue.get(observation.value()))
                .filter(Objects::nonNull)
                .map(BusinessOperation::id)
                .collect(Collectors.toSet());
        int traceCount = traceCountAfterLinking(operations, operationsCountingTrace);

        if (operations.isEmpty()) {
            CorrelationObservation first = linkedObservations.getFirst();
            BusinessOperation newOperation = new BusinessOperation(
                    newId.get(), first.observedAt(), first.observedAt(), definition.id(), null, 0, null);
            BusinessOperation created =
                    extendedBy(newOperation, linkedObservations, definition).withTraceCount(traceCount);
            workingCopy.save(created);
            workingCopy.mapValues(linkedObservations, created.id());
            return;
        }

        BusinessOperation survivor =
                operations.stream().reduce(policy::survivor).orElseThrow();
        List<BusinessOperation> absorbed = operations.stream()
                .filter(operation -> !operation.id().equals(survivor.id()))
                .toList();
        if (!absorbed.isEmpty() && !policy.allowsMerge(traceCount)) {
            absorbed.forEach(other -> workingCopy.refuseMerge(new RefusedMerge(survivor, other, traceCount)));
            operations.stream()
                    .filter(operation -> !operationsCountingTrace.contains(operation.id()))
                    .forEach(operation -> workingCopy.save(operation.withTraceCount(operation.traceCount() + 1)));
            return;
        }
        BusinessOperation widened = survivor;
        for (BusinessOperation operation : absorbed) {
            widened = widened.includeObservation(operation.firstSeen(), null)
                    .includeObservation(operation.lastSeen(), null);
            workingCopy.merge(operation, survivor.id());
        }
        workingCopy.save(extendedBy(widened, linkedObservations, definition).withTraceCount(traceCount));
        workingCopy.mapValues(linkedObservations, survivor.id());
    }

    /**
     * The operation each of the trace's values can join: the operation the value is mapped to, if the trace carried the
     * value within that operation's merge window. Values without a mapping, or mapped to an operation outside the
     * window, are absent.
     */
    private Map<String, BusinessOperation> joinableOperationByValue(
            List<CorrelationObservation> traceObservations, Duration mergeWindow, WorkingCopy workingCopy) {
        Map<String, BusinessOperation> kept = new HashMap<>();
        for (CorrelationObservation observation : traceObservations) {
            BusinessOperation current = workingCopy.operationOf(observation.value());
            if (current != null && policy.canJoin(current, observation.observedAt(), mergeWindow)) {
                kept.put(observation.value(), current);
            }
        }
        return kept;
    }

    /**
     * How many distinct traces the operation holds once the trace is linked: the trace counts of the trace's operations
     * added up, with the trace itself counted exactly once.
     *
     * <p>An operation already counts the trace if one of the trace's earlier values is mapped to it. Example: O1 and O2
     * count 1 trace each, and a new trace merges them: 1 + 1 + 1 = 3. Had O1 counted this trace already, the result
     * would be 1 + 1 - 1 + 1 = 2.
     *
     * <p>After a refused merge two operations count the same trace; if another trace merges them later, that trace is
     * counted twice. A count can therefore be slightly too high but never too low, so the size limit is never
     * exceeded.
     *
     * @param operationsCountingTrace ids of the trace's operations that already count it
     */
    private static int traceCountAfterLinking(List<BusinessOperation> operations, Set<UUID> operationsCountingTrace) {
        int tracesCounted =
                operations.stream().mapToInt(BusinessOperation::traceCount).sum();
        return tracesCounted - operationsCountingTrace.size() + 1;
    }

    /**
     * {@code operation} widened to the times of the observations. An operation without a display value takes the
     * first PRIMARY value of the observations; an operation that has one keeps it.
     */
    private static BusinessOperation extendedBy(
            BusinessOperation operation,
            List<CorrelationObservation> observationsInTimeOrder,
            BusinessOperationDefinition definition) {
        BusinessOperation result = operation;
        for (CorrelationObservation observation : observationsInTimeOrder) {
            result = result.includeObservation(observation.observedAt(), primaryValueOrNull(observation, definition));
        }
        return result;
    }

    /** The observed value if its key is PRIMARY, else {@code null}: only PRIMARY values become display values. */
    private static String primaryValueOrNull(
            CorrelationObservation observation, BusinessOperationDefinition definition) {
        boolean primary = definition.keys().stream()
                .anyMatch(key -> key.id() == observation.correlationKeyId() && key.role() == KeyRole.PRIMARY);
        return primary ? observation.value() : null;
    }

    private static Map<TraceId, List<CorrelationObservation>> byTrace(List<CorrelationObservation> observations) {
        return observations.stream().collect(Collectors.groupingBy(CorrelationObservation::traceId));
    }

    /**
     * The run's copy of the stored operations and value mappings. It starts as the input, every linked trace changes
     * it, and at the end its differences from the input are the run's result. It lives for one {@link #link} call.
     */
    private static final class WorkingCopy {

        private final Map<String, BusinessOperation> storedOperationByValue;
        private final Map<UUID, BusinessOperation> storedOperationById = new HashMap<>();
        private final Map<String, UUID> operationIdByValue = new HashMap<>();
        /** Operations created or changed in this run that are still live, in the order they were first saved. */
        private final Map<UUID, BusinessOperation> savedOperationById = new LinkedHashMap<>();
        /** Operations merged in this run, each marked with the operation that finally absorbed it. */
        private final Map<UUID, BusinessOperation> mergedOperationById = new LinkedHashMap<>();

        private final List<RefusedMerge> refusedMerges = new ArrayList<>();

        WorkingCopy(Map<String, BusinessOperation> storedOperationByValue) {
            this.storedOperationByValue = storedOperationByValue;
            storedOperationByValue.forEach((value, operation) -> {
                storedOperationById.put(operation.id(), operation);
                operationIdByValue.put(value, operation.id());
            });
        }

        /** The live operation {@code value} is mapped to now, or {@code null}. */
        BusinessOperation operationOf(String value) {
            UUID id = operationIdByValue.get(value);
            return id == null ? null : savedOperationById.getOrDefault(id, storedOperationById.get(id));
        }

        void save(BusinessOperation operation) {
            savedOperationById.put(operation.id(), operation);
        }

        void mapValues(List<CorrelationObservation> observations, UUID operationId) {
            observations.forEach(observation -> operationIdByValue.put(observation.value(), operationId));
        }

        /**
         * Marks {@code absorbed} as merged into {@code survivorId}, and moves to the survivor everything that pointed
         * to {@code absorbed}: its value mappings and the operations merged into it earlier in this run.
         */
        void merge(BusinessOperation absorbed, UUID survivorId) {
            savedOperationById.remove(absorbed.id());
            mergedOperationById.replaceAll((id, operation) ->
                    operation.mergedInto().equals(absorbed.id()) ? operation.markMergedInto(survivorId) : operation);
            mergedOperationById.put(absorbed.id(), absorbed.markMergedInto(survivorId));
            operationIdByValue.replaceAll((value, id) -> id.equals(absorbed.id()) ? survivorId : id);
        }

        void refuseMerge(RefusedMerge refusal) {
            refusedMerges.add(refusal);
        }

        /**
         * What storage must write: new live operations, stored operations that changed, stored operations that were
         * merged, and the value mappings that differ from the stored ones.
         */
        LinkingResult changes() {
            List<BusinessOperation> created = savedOperationById.values().stream()
                    .filter(operation -> !storedOperationById.containsKey(operation.id()))
                    .toList();
            List<BusinessOperation> updated = savedOperationById.values().stream()
                    .filter(operation -> storedOperationById.containsKey(operation.id()))
                    .filter(operation -> !operation.equals(storedOperationById.get(operation.id())))
                    .toList();
            List<BusinessOperation> merged = mergedOperationById.values().stream()
                    .filter(operation -> storedOperationById.containsKey(operation.id()))
                    .toList();
            Map<String, UUID> changedMappings = new HashMap<>();
            operationIdByValue.forEach((value, id) -> {
                BusinessOperation stored = storedOperationByValue.get(value);
                if (stored == null || !stored.id().equals(id)) {
                    changedMappings.put(value, id);
                }
            });
            return new LinkingResult(created, updated, merged, changedMappings, refusedMerges);
        }
    }
}
