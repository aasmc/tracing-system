package ru.aasmc.tracing.collector.domain.enrichment;

/** Why an enrichment event was rejected; the event goes to the dead-letter topic with this reason. */
public enum ExtractionFailure {
    /** A mapping marked as required found no value. */
    MISSING_REQUIRED_FIELD,
    /** A transform could not produce a value, e.g. a regular expression did not match. */
    TRANSFORM_FAILED,
    /** The value does not fit the mapping's type, e.g. {@code "abc"} for LONG or {@code 4.5} for LONG. */
    TYPE_MISMATCH
}
