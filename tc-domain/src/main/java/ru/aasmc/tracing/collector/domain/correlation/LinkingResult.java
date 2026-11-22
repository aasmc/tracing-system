package ru.aasmc.tracing.collector.domain.correlation;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * What one {@link OperationLinker#link()} call decided; the caller writes it to storage.
 *
 * @param created new operations
 * @param updated existing operations with new bounds, display value or trace count
 * @param merged operations absorbed by another one (with {@code mergedInto} set)
 * @param operationByValue the operation each correlation value now belongs to
 * @param refusedMerges merges refused by the size limit
 */
public record LinkingResult(
        List<BusinessOperation> created,
        List<BusinessOperation> updated,
        List<BusinessOperation> merged,
        Map<String, UUID> operationByValue,
        List<MergeDecision.SizeLimitExceeded> refusedMerges) {}
