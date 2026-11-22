package ru.aasmc.tracing.collector.domain.config;

/** Retention tier of a table, {@code retention_policy.tier} (05 §1, §4.12, ADR-13). */
public enum RetentionTier {
    /** Full detail, kept {@code fullDays}. */
    FULL,
    /** Digest rows, kept until {@code fullDays + digestDays}. */
    DIGEST
}
