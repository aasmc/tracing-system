package ru.aasmc.tracing.collector.domain.correlation;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * What {@link OperationForest#resolve()} asks the resolver to persist in one transaction (05 §4.11 step 4).
 *
 * <p>Invariants: every assignment points at a root operation (I1); merged operations point at a root (depth ≤ 1);
 * lists and map are immutable.
 *
 * @param created new root operations
 * @param updated existing root operations whose bounds, display value or trace count changed
 * @param merged losers with {@code mergedInto} set (including earlier losers repointed to the new survivor)
 * @param assignments new owner of every touched value
 * @param hubBlocked merges refused by the hub guard
 */
public record ForestResolution(
        List<BusinessOperation> created,
        List<BusinessOperation> updated,
        List<BusinessOperation> merged,
        Map<CorrelationNode, UUID> assignments,
        List<MergeDecision.HubBlocked> hubBlocked) {}
