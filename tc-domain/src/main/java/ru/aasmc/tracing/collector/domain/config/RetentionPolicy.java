package ru.aasmc.tracing.collector.domain.config;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;

/**
 * One row of {@code tc.retention_policy} (05 §4.12).
 *
 * <p>Invariants ({@code INVALID_RETENTION_POLICY}): the table is one of {@link #FULL_TABLES} or {@link
 * #DIGEST_TABLES} and has the matching tier; {@code retention} is at least one partition of its granularity; {@code
 * precreate >= 1 day}.
 *
 * @param tableName partitioned table
 * @param tier FULL or DIGEST
 * @param retention age at which partitions are dropped
 * @param granularity partition granularity
 * @param precreate how far ahead partitions are created
 */
public record RetentionPolicy(
        String tableName, RetentionTier tier, Duration retention, Granularity granularity, Duration precreate) {

    public static final Set<String> FULL_TABLES = Set.of(
            "span",
            "trace_attribute",
            "span_link",
            "trace_summary",
            "trace_enrichment",
            "trace_correlation",
            "business_operation");

    public static final Set<String> DIGEST_TABLES = Set.of("trace_digest", "business_operation_digest");

    /** The tier a partitioned table belongs to; empty for unknown tables. */
    public static Optional<RetentionTier> tierOf(String tableName) {
        throw new UnsupportedOperationException();
    }

    /**
     * Cross-policy invariant, checked on every change: every row carries the retention of its tier, except {@code
     * business_operation} (MONTH grain, dropped by the drop gate).
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code RETENTION_MANAGED_BY_TIER}
     */
    public static void validateSet(Collection<RetentionPolicy> all, RetentionTiers tiers) {
        throw new UnsupportedOperationException();
    }
}
