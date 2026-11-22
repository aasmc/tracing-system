package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Instant;
import java.util.UUID;

/**
 * Row of {@code tc.correlation_value}: the current owner of a {@code (type, value)} (05 §4.11). Invariant I1: always
 * points at a root operation.
 *
 * @param correlationTypeId the type
 * @param value the correlation value
 * @param operationId the owning root operation
 * @param operationFirstSeen partition key of the owning operation
 * @param updatedAt last repoint
 */
public record CorrelationValue(
        int correlationTypeId, String value, UUID operationId, Instant operationFirstSeen, Instant updatedAt) {}
