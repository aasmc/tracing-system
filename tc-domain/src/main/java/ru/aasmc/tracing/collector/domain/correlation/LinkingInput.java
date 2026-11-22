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
 * Everything one {@link OperationLinker#link} run needs, loaded by the caller from storage. The lists are immutable
 * copies.
 *
 * <p>Rules:
 *
 * <ul>
 *   <li>every observation is of {@code definition} and of one of its keys ({@code INVALID_OPERATION_DEFINITION});
 *   <li>every operation in {@code currentOperationByValue} is of {@code definition} and not merged into another one;
 *       values that share an operation share the same copy of it, not two different versions
 *       ({@code INVALID_BUSINESS_OPERATION}).
 * </ul>
 *
 * @param definition the definition whose traces are linked
 * @param newObservations observations not linked yet
 * @param earlierObservations observations of the same traces that earlier runs already linked; they show which values a
 *     trace carried before (a session id may arrive in one run, the authorization code of the same trace in the next)
 *     and whether the trace is already counted by an operation
 * @param currentOperationByValue for every value of the observations, the operation it belongs to now; values seen for
 *     the first time are absent
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
        Set<UUID> keyIds = definition.keys().stream().map(CorrelationKey::id).collect(Collectors.toSet());
        Stream.concat(newObservations.stream(), earlierObservations.stream())
                .forEach(observation -> require(
                        definition.id().equals(observation.operationDefinitionId())
                                && keyIds.contains(observation.correlationKeyId()),
                        ViolationCode.INVALID_OPERATION_DEFINITION,
                        "observation of key " + observation.correlationKeyId() + " is not of definition "
                                + definition.name()));
        Map<UUID, BusinessOperation> operationById = new HashMap<>();
        for (BusinessOperation operation : currentOperationByValue.values()) {
            require(
                    operation.mergedInto() == null && definition.id().equals(operation.operationDefinitionId()),
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
