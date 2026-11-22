package ru.aasmc.tracing.collector.domain.enrichment;

import java.util.UUID;

/**
 * Id of an enrichment source.
 *
 * @param value the id
 */
public record EnrichmentSourceId(UUID value) {}
