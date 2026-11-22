package ru.aasmc.tracing.collector.domain.enrichment;

/** Where a field mapping reads its value (05 §4.7.1). */
public enum FieldSource {
    PAYLOAD,
    HEADER,
    RECORD_KEY,
    RECORD_TIMESTAMP
}
