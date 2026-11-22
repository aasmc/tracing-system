package ru.aasmc.tracing.collector.domain.correlation;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * What one {@link OperationLinker#link} run decided; the caller writes it to storage in one transaction.
 *
 * <p>{@code operationByValue} holds the value mappings that change, and covers only values of the run's observations.
 * Storage must also change the mapping of every other value of a {@code merged} operation to the operation it was
 * merged into.
 *
 * @param created new operations
 * @param updated existing operations with new bounds, display value or trace count
 * @param merged existing operations absorbed by another one (with {@code mergedInto} set)
 * @param operationByValue new or changed value mappings: the operation each of these values belongs to from now on
 * @param refusedMerges merges refused by the size limit; the traces that would have caused them were left unlinked
 */
public record LinkingResult(
        List<BusinessOperation> created,
        List<BusinessOperation> updated,
        List<BusinessOperation> merged,
        Map<String, UUID> operationByValue,
        List<RefusedMerge> refusedMerges) {

    public LinkingResult {
        created = List.copyOf(created);
        updated = List.copyOf(updated);
        merged = List.copyOf(merged);
        operationByValue = Map.copyOf(operationByValue);
        refusedMerges = List.copyOf(refusedMerges);
    }
}
