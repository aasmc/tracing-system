package ru.aasmc.tracing.collector.domain.resource;

/**
 * Content-hash identity of an {@link InstrumentationScope} ({@link ResourceHasher}, 05 §3.5).
 *
 * @param value the hash as a signed long ({@code instrumentation_scope.id bigint})
 */
public record ScopeId(long value) {}
