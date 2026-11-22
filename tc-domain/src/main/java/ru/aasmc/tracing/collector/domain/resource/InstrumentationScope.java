package ru.aasmc.tracing.collector.domain.resource;

import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/**
 * The library that created a span, deduplicated by content hash in {@code tc.instrumentation_scope} (05 §3.5).
 *
 * <p>Invariants: {@code name} not null; {@code id} equals the content hash ({@code CONTENT_HASH_MISMATCH}).
 *
 * @param id content hash
 * @param name scope name
 * @param version scope version or {@code null}
 * @param schemaUrl schema URL or {@code null}
 * @param attributes scope attributes
 */
public record InstrumentationScope(ScopeId id, String name, String version, String schemaUrl, Attributes attributes) {

    /** Computes the id. */
    public static InstrumentationScope of(String name, String version, String schemaUrl, Attributes attributes) {
        throw new UnsupportedOperationException();
    }
}
