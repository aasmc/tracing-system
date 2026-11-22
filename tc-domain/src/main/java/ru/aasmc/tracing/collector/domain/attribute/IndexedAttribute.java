package ru.aasmc.tracing.collector.domain.attribute;

/**
 * An attribute key that is searchable: only these keys are written to the search index. A key may also be retained:
 * its values are then copied into the trace digest that is kept after the full trace data is deleted.
 *
 * <p>Rules: a built-in key (e.g. {@code service.name}) is never DISABLED ({@code BUILT_IN_KEY}); a DISABLED key is
 * never retained, because the digest is built from the index ({@code RETAINED_KEY_MUST_BE_INDEXED}); a key that is
 * not retained has transform NONE ({@code INVALID_DIGEST_TRANSFORM}).
 *
 * @param keyId dictionary id of the key
 * @param key the key
 * @param status life-cycle status
 * @param builtIn whether the key is built in
 * @param retainedInDigest whether values are copied into the digest
 * @param digestTransform how values are written into the digest
 */
public record IndexedAttribute(
        int keyId,
        AttributeKey key,
        IndexStatus status,
        boolean builtIn,
        boolean retainedInDigest,
        DigestTransform digestTransform) {

    /**
     * Moves to another status following {@link IndexStatus#transitionTo}.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException if the step is not allowed or breaks a rule
     *     above
     */
    public IndexedAttribute moveTo(IndexStatus target) {
        throw new UnsupportedOperationException();
    }

    /**
     * Starts retaining values with {@code transform}. Personal data may be kept in clear text only if the caller
     * explicitly acknowledges it ({@code PERSONAL_DATA_RETAINED_IN_CLEAR}); secrets never ({@code SECRET_RETAINED_IN_CLEAR}).
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException if a rule is broken
     */
    public IndexedAttribute retainInDigest(
            DigestTransform transform, Sensitivity sensitivity, boolean personalDataAcknowledged) {
        throw new UnsupportedOperationException();
    }
}
