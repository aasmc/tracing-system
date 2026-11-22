package ru.aasmc.tracing.collector.domain.correlation;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.time.Instant;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * The fact "trace T carries value V of correlation key K".
 *
 * <p>{@code value} is not blank; trace id and time are set ({@code INVALID_CORRELATION_VALUE}). The maximum value
 * length is configured in {@link CorrelationLimits}.
 *
 * @param observedAt when the value was seen: the span start, or the event time for enrichment
 * @param operationDefinitionId the business operation definition whose key was observed
 * @param correlationKeyId the key
 * @param value the value
 * @param traceId the trace
 */
public record CorrelationObservation(
        Instant observedAt, int operationDefinitionId, int correlationKeyId, String value, TraceId traceId) {

    public CorrelationObservation {
        require(
                observedAt != null && traceId != null,
                ViolationCode.INVALID_CORRELATION_VALUE,
                "observation without time or trace");
        require(
                value != null && !value.isBlank(),
                ViolationCode.INVALID_CORRELATION_VALUE,
                "correlation value is blank");
    }
}
