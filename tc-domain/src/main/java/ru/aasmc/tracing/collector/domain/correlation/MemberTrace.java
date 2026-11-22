package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * What compaction knows about one member trace of a business operation (05 §4.10.1): from {@code trace_digest},
 * {@code trace_attribute} and {@code trace_correlation}.
 *
 * <p>Invariants: trace id and start not null; {@code errorCount >= 0}; {@code stepLabels} is an immutable copy.
 *
 * @param traceId the trace
 * @param startTime trace start
 * @param errorCount spans with status ERROR
 * @param outcomeValue raw value of the rule's outcome key on this trace, if any
 * @param stepLabels labels of all correlation keys that linked this trace
 */
public record MemberTrace(
        TraceId traceId, Instant startTime, int errorCount, Optional<String> outcomeValue, Set<String> stepLabels) {}
