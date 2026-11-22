package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Instant;
import java.util.UUID;

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
 * @param correlationTypeId the type; operations of different types never merge
 * @param displayValue the first primary value, {@code null} while provisional
 * @param traceCount distinct member traces
 * @param mergedInto the operation that absorbed this one, or {@code null}
 */
public record BusinessOperation(
        UUID id,
        Instant firstSeen,
        Instant lastSeen,
        int correlationTypeId,
        String displayValue,
        int traceCount,
        UUID mergedInto) {

    public boolean isProvisional() {
        throw new UnsupportedOperationException();
    }

    /** Widens {@code [firstSeen, lastSeen]} to {@code observedAt} and sets the display value if it has none. */
    public BusinessOperation includeObservation(Instant observedAt, String primaryValueOrNull) {
        throw new UnsupportedOperationException();
    }

    public BusinessOperation withTraceCount(int traceCount) {
        throw new UnsupportedOperationException();
    }

    public BusinessOperation markMergedInto(UUID survivorId) {
        throw new UnsupportedOperationException();
    }
}
