package ru.aasmc.tracing.collector.domain.resource;

import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/**
 * Computes ids of resources (the process that produced spans) and instrumentation scopes from their content. Each
 * service instance sends the same resource with every span, so a content id lets storage keep one row per distinct
 * resource instead of one copy per span.
 *
 * <p>The id is the xxHash64 (seed 0) of the UTF-8 bytes of this encoding:
 *
 * <ol>
 *   <li>resource: {@code schemaUrl '\n'}; scope: {@code name '\n' version '\n' schemaUrl '\n'} ({@code ""} for
 *       {@code null});
 *   <li>then the attributes sorted by key, each as {@code key '\u0001' type '\u0001' value '\n'}, where {@code value}
 *       is the value's index tokens joined by {@code '\u0002'}.
 * </ol>
 *
 * <p>The result therefore does not depend on attribute order or on the JVM. Changing the encoding changes every id.
 */
public final class ResourceHasher {

    private ResourceHasher() {}

    public static ResourceId resourceId(String schemaUrl, Attributes attributes) {
        throw new UnsupportedOperationException();
    }

    public static InstrumentationScopeId instrumentationScopeId(
            String name, String version, String schemaUrl, Attributes attributes) {
        throw new UnsupportedOperationException();
    }
}
