package ru.aasmc.tracing.collector.domain.resource;

/**
 * Content id of a resource, computed by {@link ResourceHasher}.
 *
 * @param value the 64-bit hash
 */
public record ResourceId(long value) {}
