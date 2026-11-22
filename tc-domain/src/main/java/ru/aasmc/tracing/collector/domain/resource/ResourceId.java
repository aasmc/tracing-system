package ru.aasmc.tracing.collector.domain.resource;

/**
 * Content-hash identity of a {@link Resource}: xxHash64 of its canonical encoding ({@link ResourceHasher}, 05 §3.5).
 *
 * @param value the hash as a signed long ({@code resource.id bigint})
 */
public record ResourceId(long value) {}
