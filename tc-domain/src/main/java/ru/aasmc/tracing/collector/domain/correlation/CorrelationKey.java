package ru.aasmc.tracing.collector.domain.correlation;

import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;

/**
 * An attribute key that belongs to a correlation type (05 §4.10).
 *
 * <p>Invariants ({@code INVALID_CORRELATION_TYPE}): key and role not null; {@code stepLabel} is {@code null} or
 * 1..{@link #MAX_STEP_LABEL} characters.
 *
 * @param id {@code correlation_key.id}
 * @param keyId {@code attribute_key.id}
 * @param key the attribute key
 * @param role PRIMARY or ALIAS
 * @param stepLabel business step evidenced by the key, or {@code null}
 */
public record CorrelationKey(int id, int keyId, AttributeKey key, KeyRole role, String stepLabel) {

    public static final int MAX_STEP_LABEL = 64;
}
