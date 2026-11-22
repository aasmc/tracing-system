package ru.aasmc.tracing.collector.domain.enrichment;

/**
 * Id of an {@link EnrichmentSource} ({@code enrichment_source.id}, 05 §4.7.2).
 *
 * @param value the id
 */
public record EnrichmentSourceId(long value) {}
