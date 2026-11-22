package ru.aasmc.tracing.collector.domain.enrichment;

/** Where in a Kafka record a mapped value is read from. */
public enum FieldSource {
    /** The JSON value, at a JSON Pointer. */
    PAYLOAD,
    /** A record header. */
    HEADER,
    /** The record key. */
    RECORD_KEY,
    /** The record timestamp. */
    RECORD_TIMESTAMP
}
