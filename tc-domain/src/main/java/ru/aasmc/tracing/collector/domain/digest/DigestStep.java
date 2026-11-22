package ru.aasmc.tracing.collector.domain.digest;

import java.time.Instant;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * One element of {@code business_operation_digest.steps}, ordered by start time (05 §4.14.2).
 *
 * @param stepLabel label of the key declared last among the keys that linked the trace, or {@code null}
 * @param traceId the member trace
 * @param startTime trace start
 * @param durationMs trace duration in milliseconds
 * @param outcome trace outcome
 */
public record DigestStep(
        String stepLabel, TraceId traceId, Instant startTime, double durationMs, TraceOutcome outcome) {}
