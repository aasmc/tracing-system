package ru.aasmc.tracing.collector.domain.digest;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.UUID;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * Row of {@code tc.trace_digest}, written once by compaction (05 §4.14.2). A split trace keeps one digest row per
 * {@code trace_summary} row.
 *
 * <p>Invariants ({@code INVALID_DIGEST}): day, trace id and start not null; {@code spanCount >= 1}; {@code 0 <=
 * errorCount <= spanCount}; {@code outcome == TraceOutcome.ofErrorCount(errorCount)}; {@code durationNanos >= 0};
 * service ids and attributes are immutable copies. Attribute values are already transformed: never raw PII for
 * SHA256/MASK keys.
 *
 * @param traceDay partition key
 * @param traceId the trace
 * @param startTime trace start
 * @param durationNanos trace duration
 * @param rootServiceId root service dictionary id, {@code null} if rootless
 * @param rootNameId root name dictionary id, {@code null} if rootless or overflowed
 * @param spanCount spans
 * @param errorCount spans with status ERROR
 * @param outcome OK or ERROR
 * @param serviceIds distinct services
 * @param businessOperationId root operation of the lowest correlation type id, or {@code null}
 * @param attributes retained key → distinct transformed values
 * @param compactedAt compaction time
 */
public record TraceDigest(
        LocalDate traceDay,
        TraceId traceId,
        Instant startTime,
        long durationNanos,
        Integer rootServiceId,
        Integer rootNameId,
        int spanCount,
        int errorCount,
        TraceOutcome outcome,
        SortedSet<Integer> serviceIds,
        UUID businessOperationId,
        Map<String, List<String>> attributes,
        Instant compactedAt) {

    public boolean belongsToOperation() {
        throw new UnsupportedOperationException();
    }
}
