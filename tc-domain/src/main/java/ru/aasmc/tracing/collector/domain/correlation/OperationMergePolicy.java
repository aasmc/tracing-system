package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Duration;
import java.time.Instant;

/**
 * Rules for linking traces into operations.
 *
 * <ul>
 *   <li><b>Survivor</b>: when two operations merge, the one with a display value wins over a provisional one, then
 *       the one first seen earlier, then the smaller id. The choice does not depend on arrival order.
 *   <li><b>Merge window</b>: a value seen at {@code t} joins an operation only if {@code t} lies in
 *       {@code [firstSeen - window, lastSeen + window]}.
 *   <li><b>Size limit</b>: no merge may produce more than {@code maxTracesPerOperation} traces.
 * </ul>
 *
 * @param maxTracesPerOperation the size limit, {@code >= 1}
 */
public record OperationMergePolicy(int maxTracesPerOperation) {

    public static final OperationMergePolicy DEFAULT = new OperationMergePolicy(10_000);

    public BusinessOperation survivor(BusinessOperation a, BusinessOperation b) {
        throw new UnsupportedOperationException();
    }

    public boolean canJoin(BusinessOperation operation, Instant observedAt, Duration mergeWindow) {
        throw new UnsupportedOperationException();
    }

    /**
     * Decides the merge of two different operations of the same type.
     *
     * @param combinedTraceCount distinct traces of both together
     */
    public MergeDecision decideMerge(BusinessOperation a, BusinessOperation b, int combinedTraceCount) {
        throw new UnsupportedOperationException();
    }
}
