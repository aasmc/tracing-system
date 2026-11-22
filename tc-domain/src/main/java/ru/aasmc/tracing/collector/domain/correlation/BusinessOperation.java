package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Instant;
import java.util.UUID;

/**
 * The set of traces that represent one business action, connected through shared correlation values (05 §4.11). Row
 * of {@code tc.business_operation}; modified only by the single-leader resolver.
 *
 * <p>Invariants ({@code INVALID_BUSINESS_OPERATION}): {@code id} is a version-7 UUID; {@code firstSeen <= lastSeen}
 * (I4); {@code traceCount >= 0}; an operation is never merged into itself. Derived states (05 §4.11 diagram):
 * PROVISIONAL ({@code displayValue == null}), OPEN, MERGED ({@code mergedInto != null}); CAPPED is decided by
 * {@link OperationMergePolicy}.
 *
 * @param id uuidv7
 * @param firstSeen earliest observation
 * @param lastSeen latest observation
 * @param correlationTypeId the type (I3: types never merge)
 * @param displayValue first PRIMARY value, {@code null} while provisional
 * @param traceCount distinct member traces
 * @param mergedInto survivor id, {@code null} for a root operation
 */
public record BusinessOperation(
        UUID id,
        Instant firstSeen,
        Instant lastSeen,
        int correlationTypeId,
        String displayValue,
        int traceCount,
        UUID mergedInto) {

    public boolean isMerged() {
        throw new UnsupportedOperationException();
    }

    public boolean isProvisional() {
        throw new UnsupportedOperationException();
    }

    /** Widens {@code [firstSeen, lastSeen]} and sets the display value if it is still {@code null}. */
    public BusinessOperation observe(Instant observedAt, String primaryValueOrNull) {
        throw new UnsupportedOperationException();
    }

    public BusinessOperation withTraceCount(int traceCount) {
        throw new UnsupportedOperationException();
    }

    /**
     * Marks this operation as merged into the survivor (I2: never un-merged).
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_BUSINESS_OPERATION} if
     *     {@code survivorId} is this operation's id
     */
    public BusinessOperation mergedInto(UUID survivorId) {
        throw new UnsupportedOperationException();
    }
}
