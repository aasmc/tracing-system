package ru.aasmc.tracing.collector.domain.resource;

import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/**
 * The entity that produced telemetry, stored once per content hash in {@code tc.resource} (05 §3.5).
 *
 * <p>Invariants: {@code serviceName} not blank; {@code id} equals the content hash of {@code (schemaUrl,
 * attributes)} ({@code CONTENT_HASH_MISMATCH}).
 *
 * @param id content hash
 * @param serviceName value of {@code service.name}, or {@link #UNKNOWN_SERVICE}
 * @param schemaUrl schema URL or {@code null}
 * @param attributes resource attributes
 */
public record Resource(ResourceId id, String serviceName, String schemaUrl, Attributes attributes) {

    /** Service name used when {@code service.name} is missing, blank or not a string. */
    public static final String UNKNOWN_SERVICE = "unknown_service";

    /** Computes the id and extracts the service name. */
    public static Resource of(String schemaUrl, Attributes attributes) {
        throw new UnsupportedOperationException();
    }
}
