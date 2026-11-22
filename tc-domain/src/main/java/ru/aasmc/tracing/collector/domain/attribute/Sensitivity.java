package ru.aasmc.tracing.collector.domain.attribute;

/** How sensitive the values of an attribute key are. */
public enum Sensitivity {
    /** Not personal. */
    NONE,
    /** Personal data, e.g. a user id or a phone number: shown only to callers allowed to see personal data. */
    PERSONAL,
    /** Secrets, e.g. tokens or authorization codes: never shown in clear text. */
    SECRET
}
