package ru.aasmc.tracing.collector.domain.correlation;

/** Sealed result of {@link OperationMergePolicy#decide} (03 §6.3). */
public sealed interface MergeDecision {

    /**
     * Merge {@code loser} into {@code survivor}.
     *
     * @param survivor the operation that stays a root
     * @param loser the operation that gets {@code merged_into = survivor.id}
     */
    record Merge(BusinessOperation survivor, BusinessOperation loser) implements MergeDecision {}

    /**
     * The hub guard refused the merge: the survivor would exceed the trace limit. Both stay separate; the resolver
     * logs and counts it ({@code tc_correlation_hub_blocked_total}).
     *
     * @param survivor the operation that would have survived
     * @param other the other operation
     * @param combinedTraceCount traces the merged operation would have
     */
    record HubBlocked(BusinessOperation survivor, BusinessOperation other, int combinedTraceCount)
            implements MergeDecision {}
}
