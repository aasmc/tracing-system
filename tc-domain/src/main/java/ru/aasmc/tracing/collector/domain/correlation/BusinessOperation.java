package ru.aasmc.tracing.collector.domain.correlation;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.time.Instant;
import java.util.UUID;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * A set of traces that together perform one business action, e.g. one login.
 *
 * <p>An operation is <em>provisional</em> while it has no display value (only alias values were seen so far) and
 * <em>merged</em> once another operation absorbed it; a merged operation is never un-merged.
 *
 * <p>Rules ({@code INVALID_BUSINESS_OPERATION}): {@code id} is a version-7 UUID; {@code firstSeen <= lastSeen};
 * {@code traceCount >= 0}; an operation is never merged into itself.
 *
 * @param id time-ordered id
 * @param firstSeen earliest observation
 * @param lastSeen latest observation
 * @param operationDefinitionId the definition of the operation; operations of different definitions never merge
 * @param displayValue the first primary value, {@code null} while provisional
 * @param traceCount distinct member traces
 * @param mergedInto the operation that absorbed this one, or {@code null}
 */
public record BusinessOperation(
        UUID id,
        Instant firstSeen,
        Instant lastSeen,
        int operationDefinitionId,
        String displayValue,
        int traceCount,
        UUID mergedInto) {

    private static final int TIME_ORDERED_UUID_VERSION = 7;

    public BusinessOperation {
        require(
                id != null && id.version() == TIME_ORDERED_UUID_VERSION,
                ViolationCode.INVALID_BUSINESS_OPERATION,
                "operation id is not a version-7 UUID: " + id);
        require(
                firstSeen != null && lastSeen != null && !firstSeen.isAfter(lastSeen),
                ViolationCode.INVALID_BUSINESS_OPERATION,
                "operation " + id + " seen from " + firstSeen + " to " + lastSeen);
        require(traceCount >= 0, ViolationCode.INVALID_BUSINESS_OPERATION, "negative trace count: " + traceCount);
        require(
                !id.equals(mergedInto),
                ViolationCode.INVALID_BUSINESS_OPERATION,
                "operation " + id + " cannot be merged into itself");
    }

    public boolean isProvisional() {
        return displayValue == null;
    }

    /** Widens {@code [firstSeen, lastSeen]} to {@code observedAt} and sets the display value if it has none. */
    public BusinessOperation includeObservation(Instant observedAt, String primaryValueOrNull) {
        return new BusinessOperation(
                id,
                observedAt.isBefore(firstSeen) ? observedAt : firstSeen,
                observedAt.isAfter(lastSeen) ? observedAt : lastSeen,
                operationDefinitionId,
                isProvisional() ? primaryValueOrNull : displayValue,
                traceCount,
                mergedInto);
    }

    public BusinessOperation withTraceCount(int traceCount) {
        return new BusinessOperation(
                id, firstSeen, lastSeen, operationDefinitionId, displayValue, traceCount, mergedInto);
    }

    public BusinessOperation markMergedInto(UUID survivorId) {
        return new BusinessOperation(
                id, firstSeen, lastSeen, operationDefinitionId, displayValue, traceCount, survivorId);
    }
}
