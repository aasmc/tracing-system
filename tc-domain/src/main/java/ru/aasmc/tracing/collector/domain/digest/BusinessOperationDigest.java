package ru.aasmc.tracing.collector.domain.digest;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Row of {@code tc.business_operation_digest}: one per root business operation, written once (05 §4.14.2). The id
 * is the full-tier operation id, so links from trace digests survive.
 *
 * <p>Invariants ({@code INVALID_DIGEST}): {@code firstSeen <= lastSeen}; {@code opDay} is the UTC date of {@code
 * firstSeen}; {@code outcomeDetail} at most {@link #MAX_OUTCOME_DETAIL} characters; steps are an immutable copy
 * ordered by start time; attributes an immutable copy.
 *
 * @param opDay partition key
 * @param operationId root operation id
 * @param correlationTypeId the type
 * @param displayValue first PRIMARY value, or {@code null} for a provisional operation
 * @param firstSeen first observation
 * @param lastSeen last observation
 * @param traceCount member traces
 * @param outcome evaluated outcome
 * @param outcomeDetail raw outcome-key value or {@code null}
 * @param steps member traces as steps
 * @param attributes union of the members' retained attributes
 * @param compactedAt compaction time
 */
public record BusinessOperationDigest(
        LocalDate opDay,
        UUID operationId,
        int correlationTypeId,
        String displayValue,
        Instant firstSeen,
        Instant lastSeen,
        int traceCount,
        OperationOutcome outcome,
        String outcomeDetail,
        List<DigestStep> steps,
        Map<String, List<String>> attributes,
        Instant compactedAt) {

    public static final int MAX_OUTCOME_DETAIL = 256;
}
