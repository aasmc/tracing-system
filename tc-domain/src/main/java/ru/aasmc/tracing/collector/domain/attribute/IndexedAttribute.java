package ru.aasmc.tracing.collector.domain.attribute;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import ru.aasmc.tracing.collector.domain.ViolationCode;

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

    public IndexedAttribute {
        require(
                !(builtIn && status == IndexStatus.DISABLED),
                ViolationCode.BUILT_IN_KEY,
                "built-in key " + key.value() + " cannot be disabled");
        require(
                !(retainedInDigest && status == IndexStatus.DISABLED),
                ViolationCode.RETAINED_KEY_MUST_BE_INDEXED,
                "retained key " + key.value() + " cannot be disabled");
        require(
                retainedInDigest || digestTransform == DigestTransform.NONE,
                ViolationCode.INVALID_DIGEST_TRANSFORM,
                "key " + key.value() + " is not retained, so it has no digest transform");
    }

    /**
     * Moves to another status following {@link IndexStatus#transitionTo}.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException if the status change is not allowed or the
     *     result would break one of the rules of this class
     */
    public IndexedAttribute moveTo(IndexStatus target) {
        IndexStatus newStatus = status.transitionTo(target).resultingStateOrThrow();
        return new IndexedAttribute(keyId, key, newStatus, builtIn, retainedInDigest, digestTransform);
    }

    /**
     * Starts copying this key's values into trace digests, written with {@code transform}. Digests are kept much longer
     * than full trace data, so personal data may be kept there in clear text only if the admin explicitly acknowledges
     * it ({@code PERSONAL_DATA_RETAINED_IN_CLEAR}), and secrets never ({@code SECRET_RETAINED_IN_CLEAR}).
     *
     * @param transform how values are written into the digest
     * @param sensitivity how sensitive the key's values are
     * @param personalDataAcknowledged whether the admin accepted keeping personal data in clear text
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException if a rule is broken
     */
    public IndexedAttribute retainInDigest(
            DigestTransform transform, Sensitivity sensitivity, boolean personalDataAcknowledged) {
        boolean inClearText = transform == DigestTransform.NONE;
        require(
                !(inClearText && sensitivity == Sensitivity.SECRET),
                ViolationCode.SECRET_RETAINED_IN_CLEAR,
                "secret key " + key.value() + " cannot be retained in clear text");
        require(
                !(inClearText && sensitivity == Sensitivity.PERSONAL && !personalDataAcknowledged),
                ViolationCode.PERSONAL_DATA_RETAINED_IN_CLEAR,
                "personal key " + key.value() + " is retained in clear text only when acknowledged");
        return new IndexedAttribute(keyId, key, status, builtIn, true, transform);
    }
}
