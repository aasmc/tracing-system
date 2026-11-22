package ru.aasmc.tracing.collector.domain.correlation;

/** Role of a correlation key (05 §4.10). */
public enum KeyRole {
    /** The natural business id; the first PRIMARY value becomes the operation's display value. */
    PRIMARY,
    /** A secondary id that bridges traces (e.g. an authorization code). */
    ALIAS
}
