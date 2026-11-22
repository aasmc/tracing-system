package ru.aasmc.tracing.collector.domain.correlation;

import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * Links traces of one correlation type into business operations: two correlation values belong together when they
 * appear on the same trace, and each group of values that belong together, directly or through other values, is one
 * operation.
 *
 * <p>Usage: {@link #addExisting} the operations already stored, {@link #addObservation} the new observations, then
 * {@link #link} once. The result does not depend on the order of these calls, and observing the same fact twice changes nothing.
 * Not thread-safe: use one instance per run.
 */
public final class OperationLinker {

    /**
     * @param type the type whose observations are grouped
     * @param policy survivor, window and size rules
     * @param newId supplies ids for new operations (version-7 UUIDs)
     */
    public OperationLinker(CorrelationType type, OperationMergePolicy policy, Supplier<UUID> newId) {}

    /**
     * Adds a stored operation together with the values that point at it and its traces.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_BUSINESS_OPERATION} for a
     *     merged operation or one of another type
     */
    public void addExisting(BusinessOperation operation, Set<String> values, Set<TraceId> traces) {
        throw new UnsupportedOperationException();
    }

    /**
     * Adds a new observation.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_CORRELATION_TYPE} for another
     *     type or an unknown key
     */
    public void addObservation(CorrelationObservation observation) {
        throw new UnsupportedOperationException();
    }

    public LinkingResult link() {
        throw new UnsupportedOperationException();
    }
}
