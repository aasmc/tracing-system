package ru.aasmc.tracing.collector.domain.enrichment;

import java.time.Instant;

/**
 * Immutable, numbered version of a source's definition (05 §4.7.2); rows are never updated.
 *
 * <p>Invariants: {@code revision >= 1} ({@code INVALID_REVISION}); definition not null.
 *
 * @param sourceId owning source
 * @param revision revision number
 * @param definition the definition
 * @param createdBy actor
 * @param createdAt creation time
 */
public record SourceRevision(
        EnrichmentSourceId sourceId, int revision, SourceDefinition definition, String createdBy, Instant createdAt) {}
