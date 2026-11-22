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
 * Links the traces of one business operation definition into business operations.
 *
 * <p><b>The problem.</b> One business action, e.g. a login, often takes several requests, and each request is its own
 * trace. The traces do not reference each other, but they carry shared attribute values. The business operation
 * definition {@code idp.login} names two such attributes: the session id {@code idp.auth_session_id} (a PRIMARY key:
 * its first value becomes the operation's display name) and the authorization code {@code oauth.authorization_code}
 * (an ALIAS key: it only links). An <em>observation</em> is the fact "trace T carried value V at time t".
 *
 * <p>The rule: <b>two values belong to the same operation if one trace carried both</b>, directly or through a chain of
 * such traces. In the login, T1 carries {@code as-777}; T2 carries {@code as-777} and {@code code-abc}; T3 carries
 * {@code code-abc}. T2 ties the two values together, so T1, T2 and T3 are one operation although T1 and T3 share no
 * value.
 *
 * <p><b>What storage holds and what a run gets.</b> Storage keeps the operations and the <em>value mappings</em>: one
 * row per value saying which operation it belongs to, e.g. {@code as-777 → O1}. The linker keeps nothing between
 * calls. Every few seconds the caller loads a batch of new observations and, for them, a {@link LinkingInput}: the
 * earlier observations of the same traces and the current operation of every value involved. {@link #link} returns a
 * {@link LinkingResult} (operations to create, update and mark as merged, value mappings to write, refused merges),
 * which the caller writes back in one transaction. Runs of one business operation definition must not overlap (two
 * runs could both create an operation for the same new value), so the caller serialises them.
 *
 * <p><b>One run, step by step.</b> The run links the traces of the batch one at a time, in the order of their earliest
 * new observation, exactly as if each trace came in a run of its own. A run starts from a working copy of what it was
 * given and updates it after every trace, so a later trace of the batch sees what an earlier one changed. For each
 * trace:
 *
 * <ol>
 *   <li><b>Find the operation each value keeps</b> ({@code keptOperationByValue}). A value keeps its current operation
 *       if the trace carried it within that operation's merge window. A value seen outside the window is treated as
 *       reused (e.g. a recycled session id) and behaves like a value never seen before, unless the trace already
 *       carried it in an earlier run: then the value was reused <em>since</em>, by another operation, and is left
 *       out, so a late observation of an old trace never takes a value away from the operation that uses it now.
 *   <li><b>Link the trace</b> ({@code linkTrace}). The trace's operations are the operations its values keep:
 *       <ul>
 *         <li>no operation: create one;
 *         <li>one operation: extend it (time bounds, display name, trace count);
 *         <li>several operations: merge them into the {@linkplain OperationMergePolicy#survivor survivor}, which is
 *             extended, while the others are marked as merged into it; if the merged operation would hold more traces
 *             than the size limit allows, the trace changes nothing and the refusal is reported.
 *       </ul>
 *   <li><b>Map the trace's values</b> to the resulting operation.
 * </ol>
 *
 * At the end the result is the difference between the working copy and the input: operations created, operations
 * changed, operations merged and value mappings changed. An operation created and merged within the same run never
 * reaches storage.
 *
 * <p><b>Worked example</b> (merge window one day; all times on 2026-11-21). The login arrives as T1, then T3, then T2.
 *
 * <ul>
 *   <li><b>Run 1.</b> New: T1 carried {@code as-777} at 09:15:02. Nothing is stored yet.
 *       <ol>
 *         <li>{@code as-777} has no operation.
 *         <li>No operation, so create O1: seen 09:15:02..09:15:02, display name {@code as-777} (a PRIMARY value), 1
 *             trace.
 *         <li>New mapping {@code as-777 → O1}.
 *       </ol>
 *   <li><b>Run 2.</b> New: T3 carried {@code code-abc} at 09:15:33. T3 has no earlier observations;
 *       {@code code-abc} has no operation.
 *       <ol>
 *         <li>{@code code-abc} has no operation.
 *         <li>Create O2: seen 09:15:33, no display name (only an ALIAS value was seen, so it is provisional), 1 trace.
 *         <li>New mapping {@code code-abc → O2}.
 *       </ol>
 *   <li><b>Run 3.</b> New: T2 carried {@code as-777} and {@code code-abc} at 09:15:31. Current operations:
 *       {@code as-777 → O1}, {@code code-abc → O2}.
 *       <ol>
 *         <li>09:15:31 lies within one day of both operations, so both values keep them.
 *         <li>T2's operations are O1 and O2: a merge. The merged operation holds O1's 1 trace, O2's 1 trace and T2,
 *             which neither holds yet: 3 traces, within the limit. O1 survives (a named operation wins over a
 *             provisional one); it is widened to 09:15:02..09:15:33 with 3 traces. O2 is marked as merged into O1.
 *         <li>The mapping of {@code code-abc} changes to O1; {@code as-777} is already mapped to O1.
 *       </ol>
 *   <li><b>Run 3 again</b> (a replay). Now both values are mapped to O1, and the earlier observations of T2 show that
 *       T2 is already in O1. One operation, still 3 traces and the same bounds: the result is empty.
 *   <li><b>Run 4, three days later.</b> New: T4 carried {@code as-777}. {@code as-777} is mapped to O1, but O1 was last
 *       seen three days ago, outside the window, and T4 never carried {@code as-777} before, so the value counts as
 *       new: create O3 and change the mapping to {@code as-777 → O3}. O1 is not changed.
 *   <li><b>Run 5, a late fragment of T1.</b> New: T1 carried {@code code-xyz} at 09:15:40 on 2026-11-21; T1's earlier
 *       observation is {@code as-777} at 09:15:02. {@code as-777} is now mapped to O3, three days away, and T1 carried it
 *       before, so it was reused since: it is left out and stays with O3. {@code code-xyz} alone creates a provisional
 *       operation. (Storage only knows the current operation of each value, so the link back to O1 is lost.)
 *   <li><b>With a size limit of 2</b>, run 3 would need 3 traces: the merge is refused, O1 and O2 stay as they are, no
 *       mapping changes, and the refusal is in the result.
 * </ul>
 *
 * <p>Because traces are linked one at a time in time order, the outcome is the same whether the traces come in one
 * run or in several runs in that order, and linking the same observations again changes nothing. Safe to share
 * between threads.
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

    /** Links the traces of the batch one at a time, as described in the class documentation. */
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
     * Links one trace: creates, extends or merges its operation and maps its values to it.
     *
     * <ol>
     *   <li>The trace's observations take part, except those of values reused since: values the trace carried in an
     *       earlier run whose current operation is now outside the merge window.
     *   <li>The trace's operations are the distinct operations its values keep.
     *   <li>No operation: create one from the observations (bounds, display name, 1 trace).
     *   <li>Otherwise pick the survivor. If there are other operations and the merged operation would exceed the size
     *       limit, report the refusal and change nothing. Else mark the others as merged into the survivor, widen the
     *       survivor to their bounds and to the observations, and set its trace count.
     *   <li>Map every value of the observations to the resulting operation.
     * </ol>
     */
    private void linkTrace(
            List<CorrelationObservation> newObservations,
            List<CorrelationObservation> earlierObservations,
            BusinessOperationDefinition definition,
            WorkingCopy workingCopy) {
        List<CorrelationObservation> traceObservations = Stream.concat(
                        newObservations.stream(), earlierObservations.stream())
                .toList();
        Map<String, BusinessOperation> keptOperationByValue =
                keptOperationByValue(traceObservations, definition.mergeWindow(), workingCopy);
        Set<String> valuesReusedSince = earlierObservations.stream()
                .map(CorrelationObservation::value)
                .filter(value -> workingCopy.operationOf(value) != null && !keptOperationByValue.containsKey(value))
                .collect(Collectors.toSet());

        List<CorrelationObservation> linkedObservations = traceObservations.stream()
                .filter(observation -> !valuesReusedSince.contains(observation.value()))
                .sorted(IN_TIME_ORDER)
                .toList();
        if (linkedObservations.isEmpty()) {
            return;
        }
        List<BusinessOperation> operations = linkedObservations.stream()
                .map(observation -> keptOperationByValue.get(observation.value()))
                .filter(Objects::nonNull)
                .distinct()
                .toList();
        int traceCount = traceCountAfterLinking(operations, earlierObservations, keptOperationByValue);

        if (operations.isEmpty()) {
            CorrelationObservation first = linkedObservations.getFirst();
            BusinessOperation newOperation = new BusinessOperation(
                    newId.get(), first.observedAt(), first.observedAt(), definition.id(), null, 0, null);
            BusinessOperation created =
                    including(newOperation, linkedObservations, definition).withTraceCount(traceCount);
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
            return;
        }
        BusinessOperation widened = survivor;
        for (BusinessOperation operation : absorbed) {
            widened = widened.includeObservation(operation.firstSeen(), null)
                    .includeObservation(operation.lastSeen(), null);
            workingCopy.merge(operation, survivor.id());
        }
        workingCopy.save(including(widened, linkedObservations, definition).withTraceCount(traceCount));
        workingCopy.mapValues(linkedObservations, survivor.id());
    }

    /**
     * Step 1 for one trace: the operation each of its values keeps. A value keeps its current operation if one of the
     * trace's observations of it lies within that operation's merge window, i.e. in
     * {@code [firstSeen - window, lastSeen + window]}. A value with a current operation that it does not keep is either
     * reused by this trace (seen on it for the first time: the trace starts a new operation for it) or reused since (the
     * trace carried it in an earlier run: {@code linkTrace} leaves it out).
     *
     * <p>Example: {@code as-777} is mapped to O1, last seen at 09:15:33 on 2026-11-21, window one day. A new trace
     * carrying it at 09:15:31 the same day keeps O1; a new trace carrying it three days later does not.
     *
     * @return the kept operation of each value; values with no operation, or whose operation is not kept, are absent
     */
    private Map<String, BusinessOperation> keptOperationByValue(
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
     * How many distinct traces the trace's operation holds after linking it: the traces its operations hold already,
     * plus 1 for the trace itself unless one of them holds it already.
     *
     * <p>A trace is held already if one of its earlier observations has a value that keeps one of the operations: an
     * earlier run linked it. Example (run 3 of the class documentation): O1 holds 1 trace (T1), O2 holds 1 (T3); T2 has
     * no earlier observations, so the result is 1 + 1 + 1 = 3. In the replay, T2's earlier observations have values
     * mapped to O1, so T2 is held already and the result stays 3.
     *
     * <p>A trace held by two of the operations (possible after a refused merge) is counted twice, so the count may be
     * slightly too high, never too low: the size limit is never exceeded.
     */
    private static int traceCountAfterLinking(
            List<BusinessOperation> operations,
            List<CorrelationObservation> earlierObservations,
            Map<String, BusinessOperation> keptOperationByValue) {
        int tracesHeld =
                operations.stream().mapToInt(BusinessOperation::traceCount).sum();
        boolean heldAlready = earlierObservations.stream()
                .anyMatch(observation -> keptOperationByValue.containsKey(observation.value()));
        return heldAlready ? tracesHeld : tracesHeld + 1;
    }

    /**
     * {@code operation} widened to the times of the observations. A provisional operation (no display name) takes the
     * first PRIMARY value in time order as its name; a named operation keeps its name. Example: O2 is provisional
     * because it was created from {@code code-abc}, an ALIAS value; a later observation of {@code as-777} names it.
     */
    private static BusinessOperation including(
            BusinessOperation operation,
            List<CorrelationObservation> observationsInTimeOrder,
            BusinessOperationDefinition definition) {
        BusinessOperation result = operation;
        for (CorrelationObservation observation : observationsInTimeOrder) {
            result = result.includeObservation(observation.observedAt(), primaryValueOrNull(observation, definition));
        }
        return result;
    }

    /** The observed value if its key is PRIMARY (such a value may name an operation), else {@code null}. */
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
