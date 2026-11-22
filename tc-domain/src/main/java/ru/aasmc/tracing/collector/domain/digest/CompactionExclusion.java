package ru.aasmc.tracing.collector.domain.digest;

import java.time.Instant;

/**
 * Row of {@code tc.compaction_exclusion}: traces whose root matches are not digested unless they belong to a
 * business operation (05 §4.14.3, 03 §5.9).
 *
 * <p>Invariants: {@code rootNamePattern} is 1..256 characters ({@code INVALID_DIGEST}); a pattern made only of
 * {@code %} characters matches every root name and is rejected ({@code PATTERN_TOO_BROAD}) — with or without a
 * service name; to drop all digests set {@code digestDays = 0}.
 *
 * @param id row id
 * @param serviceName service to match, {@code null} for any
 * @param rootNamePattern SQL LIKE pattern for the root span name
 * @param description free text
 * @param createdBy actor
 * @param createdAt creation time
 */
public record CompactionExclusion(
        int id, String serviceName, String rootNamePattern, String description, String createdBy, Instant createdAt) {

    public static final int MAX_PATTERN_LENGTH = 256;
}
