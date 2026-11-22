package ru.aasmc.tracing.collector.domain.attribute;

import java.time.Instant;

/**
 * An attribute key registered for search; only these keys are written to {@code trace_attribute} (05 §4.9). Retained
 * keys are additionally copied into the digest tier at compaction (ADR-13).
 *
 * <p>Invariants: built-in keys are never DISABLED ({@code BUILT_IN_KEY}); a DISABLED key is never retained
 * ({@code RETAINED_KEY_MUST_BE_INDEXED}), because compaction reads {@code trace_attribute}, never spans; a
 * non-retained key has transform NONE ({@code INVALID_DIGEST_TRANSFORM}); a {@code null} transform becomes NONE.
 * Transitions follow {@link IndexStatus#transitionTo}.
 *
 * @param keyId {@code attribute_key.id}
 * @param key the key
 * @param originScope accepted origins
 * @param numeric whether {@code value_num} is written
 * @param status lifecycle status
 * @param indexedSince when indexing (re)started
 * @param backfillCursor lower bound of the oldest span partition already backfilled, {@code null} before the first
 *     backfill chunk (05 §4.9 note)
 * @param builtIn seeded built-in key
 * @param retainInDigest copied into {@code trace_digest.attributes}
 * @param digestTransform transform applied at compaction
 */
public record IndexedAttribute(
        int keyId,
        AttributeKey key,
        OriginScope originScope,
        boolean numeric,
        IndexStatus status,
        Instant indexedSince,
        Instant backfillCursor,
        boolean builtIn,
        boolean retainInDigest,
        DigestTransform digestTransform) {

    /** A new registration: BACKFILLING, not retained, {@code indexedSince = now}. */
    public static IndexedAttribute register(
            int keyId, AttributeKey key, OriginScope originScope, boolean numeric, Instant now) {
        throw new UnsupportedOperationException();
    }

    /**
     * Flags the key for the digest (not retroactive: affects trace days not yet DONE).
     *
     * @param transform digest transform
     * @param piiClass classification of the key
     * @param acknowledgePii admin confirmed that a PERSONAL value may be kept in clear
     * @param force admin confirmed a transform change of an already retained key
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code RETAINED_KEY_MUST_BE_INDEXED} if
     *     DISABLED; {@code SECRET_KEY_REQUIRES_TRANSFORM} for SECRET with NONE; {@code PII_RETAINED_IN_CLEAR} for
     *     PERSONAL with NONE without acknowledgement; {@code DIGEST_TRANSFORM_CHANGE} for a changed transform of a
     *     retained key without {@code force}
     */
    public IndexedAttribute retain(
            DigestTransform transform, PiiClass piiClass, boolean acknowledgePii, boolean force) {
        throw new UnsupportedOperationException();
    }

    /** Clears the flag and resets the transform to NONE. */
    public IndexedAttribute stopRetaining() {
        throw new UnsupportedOperationException();
    }

    /** BACKFILLING and ACTIVE keys are written by ingest and enrichment for new data. */
    public boolean writtenByIngest() {
        throw new UnsupportedOperationException();
    }

    /** ANY accepts every origin except DERIVED (unless built-in); the other scopes accept only their origin. */
    public boolean acceptsOrigin(AttributeOrigin origin) {
        throw new UnsupportedOperationException();
    }

    /** Records backfill progress. */
    public IndexedAttribute backfillProgressed(Instant cursor) {
        throw new UnsupportedOperationException();
    }

    /**
     * BACKFILLING → ACTIVE.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_INDEX_TRANSITION}
     */
    public IndexedAttribute backfillCompleted() {
        throw new UnsupportedOperationException();
    }

    /**
     * → DISABLED; also stops retention.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code BUILT_IN_KEY} for built-ins
     */
    public IndexedAttribute disable() {
        throw new UnsupportedOperationException();
    }

    /**
     * DISABLED → BACKFILLING with {@code indexedSince = now} and a reset cursor.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_INDEX_TRANSITION}
     */
    public IndexedAttribute reEnable(Instant now) {
        throw new UnsupportedOperationException();
    }
}
