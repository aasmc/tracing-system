package ru.aasmc.tracing.collector.domain.correlation;

import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;

/**
 * An attribute whose value links traces into one business operation.
 *
 * <p>Key and role are set; {@code stepLabel} is {@code null} or 1..{@value #MAX_STEP_LABEL} characters
 * ({@code INVALID_CORRELATION_TYPE}).
 *
 * @param id key id
 * @param key the attribute
 * @param role primary or alias
 * @param stepLabel the business step a trace carrying this key performs, e.g. {@code token-exchange}, or
 *     {@code null}
 */
public record CorrelationKey(int id, AttributeKey key, KeyRole role, String stepLabel) {

    public static final int MAX_STEP_LABEL = 64;
}
