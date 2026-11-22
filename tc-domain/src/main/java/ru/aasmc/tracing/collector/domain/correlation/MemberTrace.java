package ru.aasmc.tracing.collector.domain.correlation;

import java.time.Instant;
import java.util.Set;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * What outcome evaluation needs to know about one trace of a business operation.
 *
 * @param traceId the trace
 * @param startTime trace start
 * @param errorCount spans with status ERROR, {@code >= 0}
 * @param outcomeValue the trace's value of the rule's outcome attribute, or {@code null}
 * @param stepLabels the steps the trace took part in: the step labels of the correlation keys it carries; an immutable
 *     copy
 */
public record MemberTrace(
        TraceId traceId, Instant startTime, int errorCount, String outcomeValue, Set<String> stepLabels) {

    public MemberTrace {
        stepLabels = Set.copyOf(stepLabels);
    }
}
