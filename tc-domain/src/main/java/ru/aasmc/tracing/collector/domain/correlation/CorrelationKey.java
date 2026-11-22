package ru.aasmc.tracing.collector.domain.correlation;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;

/**
 * An attribute whose value links traces into one business operation.
 *
 * <p>Key and role are set; {@code stepLabel} is {@code null} or not blank ({@code INVALID_OPERATION_DEFINITION}); its
 * maximum length is configured in {@link CorrelationLimits}.
 *
 * @param id key id
 * @param key the attribute
 * @param role primary or alias
 * @param stepLabel the business step a trace carrying this key performs, e.g. {@code token-exchange}, or
 *     {@code null}
 */
public record CorrelationKey(int id, AttributeKey key, KeyRole role, String stepLabel) {

    public CorrelationKey {
        require(
                key != null && role != null,
                ViolationCode.INVALID_OPERATION_DEFINITION,
                "key without attribute or role");
        require(
                stepLabel == null || !stepLabel.isBlank(),
                ViolationCode.INVALID_OPERATION_DEFINITION,
                "blank step label");
    }
}
