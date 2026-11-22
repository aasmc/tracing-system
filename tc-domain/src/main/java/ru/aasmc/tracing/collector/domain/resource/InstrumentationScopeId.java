package ru.aasmc.tracing.collector.domain.resource;

/**
 * Content id of an instrumentation scope (the library that created a span), computed by {@link ResourceHasher}.
 *
 * @param value the 64-bit hash
 */
public record InstrumentationScopeId(long value) {}
