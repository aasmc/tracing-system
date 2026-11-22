package ru.aasmc.tracing.collector.domain.config;

import java.time.Duration;

/**
 * Two-tier retention (05 §4.12, ADR-13, 03 §5.9 R1–R3, O5): the FULL tier is kept {@code fullDays} (n); the DIGEST
 * tier {@code digestDays} (m) after it, so a digest day is dropped when older than n + m days.
 *
 * <p>Invariants: {@code fullDays >= MIN_FULL_DAYS} ({@code FULL_DAYS_TOO_SMALL}); {@code digestDays >= 0} ({@code
 * INVALID_DIGEST_DAYS}; 0 disables the digest tier).
 *
 * @param fullDays n
 * @param digestDays m
 */
public record RetentionTiers(int fullDays, int digestDays) {

    /** Compaction needs one whole closed day plus slack. */
    public static final int MIN_FULL_DAYS = 2;

    /** Seed defaults (changelog 005). */
    public static final RetentionTiers DEFAULT = new RetentionTiers(7, 90);

    /**
     * R2: {@code fullDays} must exceed the largest merge window rounded up to whole days.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code FULL_DAYS_BELOW_MERGE_WINDOW}
     */
    public void requireLongerThan(Duration maxMergeWindow) {
        throw new UnsupportedOperationException();
    }

    /**
     * O5: a merge window may be at most {@code fullDays - 1 day}.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_MERGE_WINDOW}
     */
    public void requireMergeWindowFits(Duration mergeWindow) {
        throw new UnsupportedOperationException();
    }

    public boolean digestEnabled() {
        throw new UnsupportedOperationException();
    }

    /** n days. */
    public Duration fullRetention() {
        throw new UnsupportedOperationException();
    }

    /** n + m days: the value stored in the DIGEST rows of {@code retention_policy}. */
    public Duration digestDropAge() {
        throw new UnsupportedOperationException();
    }

    /** The retention every row of {@code tier} must have. */
    public Duration retentionOf(RetentionTier tier) {
        throw new UnsupportedOperationException();
    }
}
