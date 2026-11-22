package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import ru.aasmc.tracing.collector.domain.Limits;

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
 * @param maxTracesPerOperation the configurable size limit, {@code >= 1}; {@link #DEFAULT} holds the value used when
 *     none is configured
 */
public record OperationMergePolicy(int maxTracesPerOperation) {

    public static final OperationMergePolicy DEFAULT = new OperationMergePolicy(10_000);

    /** Survivors first: named before provisional, then first seen earlier, then the smaller id (as PostgreSQL orders uuid). */
    private static final Comparator<BusinessOperation> SURVIVOR_FIRST = Comparator.comparing(
                    BusinessOperation::isProvisional)
            .thenComparing(BusinessOperation::firstSeen)
            .thenComparing(operation -> operation.id().getMostSignificantBits(), Long::compareUnsigned)
            .thenComparing(operation -> operation.id().getLeastSignificantBits(), Long::compareUnsigned);

    public OperationMergePolicy {
        Limits.requirePositive("maxTracesPerOperation", maxTracesPerOperation);
    }

    public BusinessOperation survivor(BusinessOperation a, BusinessOperation b) {
        return SURVIVOR_FIRST.compare(a, b) <= 0 ? a : b;
    }

    public boolean canJoin(BusinessOperation operation, Instant observedAt, Duration mergeWindow) {
        return !observedAt.isBefore(operation.firstSeen().minus(mergeWindow))
                && !observedAt.isAfter(operation.lastSeen().plus(mergeWindow));
    }

    /** Whether a merge may produce an operation holding {@code combinedTraceCount} traces. */
    public boolean allowsMerge(int combinedTraceCount) {
        return combinedTraceCount <= maxTracesPerOperation;
    }
}
