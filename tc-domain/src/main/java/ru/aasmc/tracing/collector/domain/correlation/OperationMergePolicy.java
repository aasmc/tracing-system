package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Duration;
import java.time.Instant;

/**
 * Rules of the union-find resolver (05 §4.11, 03 §6.3).
 *
 * <ul>
 *   <li><b>Survivor</b> (deterministic, order-independent): an operation with a display value beats a provisional
 *       one; otherwise the earlier {@code firstSeen}; otherwise the smaller id.
 *   <li><b>Merge window</b>: an observation may join an operation only if {@code observedAt} lies in {@code
 *       [firstSeen - window, lastSeen + window]}; otherwise the value is treated as reused and starts a new
 *       operation.
 *   <li><b>Hub guard</b>: no merge may produce more than {@code maxTracesPerOperation} traces.
 * </ul>
 *
 * <p>Invariant: {@code maxTracesPerOperation >= 1}. Immutable and thread-safe.
 *
 * @param maxTracesPerOperation {@code tc.correlation.max-traces-per-operation}
 */
public record OperationMergePolicy(int maxTracesPerOperation) {

    public static final OperationMergePolicy DEFAULT = new OperationMergePolicy(10_000);

    public BusinessOperation survivor(BusinessOperation a, BusinessOperation b) {
        throw new UnsupportedOperationException();
    }

    public boolean withinWindow(BusinessOperation operation, Instant observedAt, Duration mergeWindow) {
        throw new UnsupportedOperationException();
    }

    /**
     * Decides a merge of two different root operations of the same type.
     *
     * @param combinedTraceCount distinct traces of the union
     */
    public MergeDecision decide(BusinessOperation a, BusinessOperation b, int combinedTraceCount) {
        throw new UnsupportedOperationException();
    }
}
