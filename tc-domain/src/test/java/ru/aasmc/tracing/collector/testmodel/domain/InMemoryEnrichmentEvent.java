package ru.aasmc.tracing.collector.testmodel.domain;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import ru.aasmc.tracing.collector.domain.enrichment.EnrichmentEvent;
import ru.aasmc.tracing.collector.domain.enrichment.JsonPointer;
import ru.aasmc.tracing.collector.domain.enrichment.JsonScalar;

/**
 * An enrichment event held in memory. Start with {@link #anEvent()} (no headers, no key, no payload, timestamp
 * {@link #TIMESTAMP}) and add what a test needs.
 *
 * @param headers header values by name
 * @param key the record key, or {@code null}
 * @param timestamp the record timestamp
 * @param payload payload scalars by JSON Pointer text, e.g. {@code /status}
 */
public record InMemoryEnrichmentEvent(
        Map<String, String> headers, String key, Instant timestamp, Map<String, JsonScalar> payload)
        implements EnrichmentEvent {

    public static final Instant TIMESTAMP = Instant.parse("2026-11-21T09:15:40Z");

    public static InMemoryEnrichmentEvent anEvent() {
        return new InMemoryEnrichmentEvent(Map.of(), null, TIMESTAMP, Map.of());
    }

    public InMemoryEnrichmentEvent withHeader(String name, String value) {
        Map<String, String> newHeaders = new HashMap<>(headers);
        newHeaders.put(name, value);
        return new InMemoryEnrichmentEvent(Map.copyOf(newHeaders), key, timestamp, payload);
    }

    public InMemoryEnrichmentEvent withKey(String value) {
        return new InMemoryEnrichmentEvent(headers, value, timestamp, payload);
    }

    public InMemoryEnrichmentEvent withPayload(String pointer, String text) {
        return withPayload(pointer, new JsonScalar.StringScalar(text));
    }

    public InMemoryEnrichmentEvent withPayload(String pointer, JsonScalar value) {
        Map<String, JsonScalar> newPayload = new HashMap<>(payload);
        newPayload.put(pointer, value);
        return new InMemoryEnrichmentEvent(headers, key, timestamp, Map.copyOf(newPayload));
    }

    @Override
    public Optional<String> header(String name) {
        return Optional.ofNullable(headers.get(name));
    }

    @Override
    public Optional<String> recordKey() {
        return Optional.ofNullable(key);
    }

    @Override
    public Instant recordTimestamp() {
        return timestamp;
    }

    @Override
    public Optional<JsonScalar> payloadValue(JsonPointer pointer) {
        return Optional.ofNullable(payload.get(pointer.value()));
    }
}
