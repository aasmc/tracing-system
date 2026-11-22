package ru.aasmc.tracing.collector.domain.resource;

import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/**
 * Content hash of resources and scopes (05 §3.5, ADR-06); named {@code ContentHash} in the 05 sketch.
 *
 * <p>Canonical encoding (part of the domain contract; a change creates new {@code resource} rows and would be
 * versioned through the seed):
 *
 * <ol>
 *   <li>resource: {@code schemaUrl} ({@code ""} if null), then {@code '\n'};
 *   <li>scope: {@code name '\n' version '\n' schemaUrl '\n'} ({@code ""} for nulls);
 *   <li>then the attributes sorted by key, each as {@code key '\u0001' typeName '\u0001' canonicalValue '\n'}, where
 *       {@code canonicalValue} is the index tokens joined by {@code '\u0002'}.
 * </ol>
 *
 * <p>The UTF-8 bytes are hashed with xxHash64, seed {@link #SEED}. The result is stable across JVMs and attribute
 * order. Stateless and thread-safe.
 */
public final class ResourceHasher {

    /** Encoding version, used as the hash seed. */
    public static final long SEED = 0L;

    private ResourceHasher() {}

    public static ResourceId resourceId(String schemaUrl, Attributes attributes) {
        throw new UnsupportedOperationException();
    }

    public static ScopeId scopeId(String name, String version, String schemaUrl, Attributes attributes) {
        throw new UnsupportedOperationException();
    }

    /** The canonical encoding of a resource, exposed for golden tests. */
    public static byte[] canonicalResource(String schemaUrl, Attributes attributes) {
        throw new UnsupportedOperationException();
    }

    /** xxHash64 of {@code bytes} with {@link #SEED}. */
    public static long xxHash64(byte[] bytes) {
        throw new UnsupportedOperationException();
    }
}
