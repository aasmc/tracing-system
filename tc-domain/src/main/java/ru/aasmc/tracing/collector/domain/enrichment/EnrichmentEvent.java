package ru.aasmc.tracing.collector.domain.enrichment;

import java.time.Instant;
import java.util.Optional;

/**
 * Read access to one event of an enrichment source, e.g. an SMS delivery report read from the client's Kafka topic.
 *
 * <p>Domain rules decide <em>where</em> to look (a header name, a JSON Pointer, the record key or timestamp); an
 * implementation outside the domain knows <em>how</em> to read a Kafka record and parse its JSON payload.
 */
public interface EnrichmentEvent {

    /** The value of the last header named {@code name}, as UTF-8 text; empty if there is no such header. */
    Optional<String> header(String name);

    /** The record key as UTF-8 text; empty if the record has no key. */
    Optional<String> recordKey();

    /** When the record was produced (the Kafka record timestamp). */
    Instant recordTimestamp();

    /**
     * The scalar at {@code pointer} in the JSON payload; empty if the payload is not JSON, the pointer leads nowhere or
     * the value there is an object, an array or {@code null}.
     */
    Optional<JsonScalar> payloadValue(JsonPointer pointer);
}
