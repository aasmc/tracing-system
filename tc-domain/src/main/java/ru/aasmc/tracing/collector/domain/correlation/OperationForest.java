package ru.aasmc.tracing.collector.domain.correlation;

import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * In-memory disjoint-set forest of one resolver tick for one correlation type (05 §4.11, 03 §6.3). Nodes are
 * {@link CorrelationNode}s; two nodes are connected when they occur on the same trace. Union uses the survivor rule
 * of {@link OperationMergePolicy} as rank, with path compression.
 *
 * <p>Contract: the result is independent of the order of {@link #load} and {@link #observe} calls (D-11);
 * re-observing an observation already applied changes nothing (I5); different types are never connected (I3).
 * Not thread-safe: one instance per tick and type.
 */
public final class OperationForest {

    /**
     * @param type the correlation type (merge window, keys and their roles)
     * @param policy survivor, window and hub-guard rules
     * @param uuidV7 id generator for new operations (must return version-7 UUIDs)
     */
    public OperationForest(CorrelationType type, OperationMergePolicy policy, Supplier<UUID> uuidV7) {
        throw new UnsupportedOperationException();
    }

    /**
     * Registers a persisted root operation, the values that currently point at it and its member traces.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_BUSINESS_OPERATION} for a
     *     merged operation or one of another type
     */
    public void load(BusinessOperation root, Set<String> values, Set<TraceId> traces) {
        throw new UnsupportedOperationException();
    }

    /**
     * Adds one observation of this type; its key decides PRIMARY or ALIAS.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_CORRELATION_TYPE} for an
     *     observation of another type or an unknown key
     */
    public void observe(CorrelationObservation observation) {
        throw new UnsupportedOperationException();
    }

    /** Computes the operations to create, update and merge. May be called once. */
    public ForestResolution resolve() {
        throw new UnsupportedOperationException();
    }
}
