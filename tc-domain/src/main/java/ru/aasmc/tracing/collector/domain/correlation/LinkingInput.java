package ru.aasmc.tracing.collector.domain.correlation;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * What one {@link OperationLinker#link} run looks at. The caller loads it from storage; the lists are immutable copies.
 *
 * <p>Rules: observations belong to {@code definition} and use one of its keys ({@code INVALID_OPERATION_DEFINITION}); the
 * operations are live (not merged) and of {@code definition}, and values mapped to the same operation id carry the same
 * version of it ({@code INVALID_BUSINESS_OPERATION}).
 *
 * @param definition the business operation definition of everything below
 * @param newObservations observations not linked yet
 * @param earlierObservations observations of the same traces that earlier runs already linked; they show which values a
 *     trace carried before (a session id may arrive in one run, the authorization code of the same trace in the next)
 *     and whether the trace is already counted by an operation
 * @param currentOperationByValue the current value mappings of the observations' values: the live
 *     operation each value belongs to now; values seen for the first time are absent
 */
public record LinkingInput(
        BusinessOperationDefinition definition,
        List<CorrelationObservation> newObservations,
        List<CorrelationObservation> earlierObservations,
        Map<String, BusinessOperation> currentOperationByValue) {

    public LinkingInput {
        newObservations = List.copyOf(newObservations);
        earlierObservations = List.copyOf(earlierObservations);
        currentOperationByValue = Map.copyOf(currentOperationByValue);
        Set<Integer> keyIds = definition.keys().stream().map(CorrelationKey::id).collect(Collectors.toSet());
        Stream.concat(newObservations.stream(), earlierObservations.stream())
                .forEach(observation -> require(
                        observation.operationDefinitionId() == definition.id()
                                && keyIds.contains(observation.correlationKeyId()),
                        ViolationCode.INVALID_OPERATION_DEFINITION,
                        "observation of key " + observation.correlationKeyId() + " is not of definition "
                                + definition.name()));
        Map<UUID, BusinessOperation> operationById = new HashMap<>();
        for (BusinessOperation operation : currentOperationByValue.values()) {
            require(
                    operation.mergedInto() == null && operation.operationDefinitionId() == definition.id(),
                    ViolationCode.INVALID_BUSINESS_OPERATION,
                    "operation " + operation.id() + " is merged or not of definition " + definition.name());
            BusinessOperation sameId = operationById.putIfAbsent(operation.id(), operation);
            require(
                    sameId == null || sameId.equals(operation),
                    ViolationCode.INVALID_BUSINESS_OPERATION,
                    "two different versions of operation " + operation.id());
        }
    }
}
