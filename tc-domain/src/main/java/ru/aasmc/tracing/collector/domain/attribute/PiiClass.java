package ru.aasmc.tracing.collector.domain.attribute;

/** Sensitivity of an attribute key; drives masking in the Query context (05 §7.3). */
public enum PiiClass {
    /** Shown to every {@code TC_VIEWER}. */
    NONE,
    /** Shown only with scope {@code TC_PII}; masked otherwise. */
    PERSONAL,
    /** Always masked, even with {@code TC_PII}; usable for EQ search and correlation only. */
    SECRET
}
