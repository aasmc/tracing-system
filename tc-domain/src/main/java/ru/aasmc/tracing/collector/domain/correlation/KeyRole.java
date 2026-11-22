package ru.aasmc.tracing.collector.domain.correlation;

/** Role of a correlation key in a business operation. */
public enum KeyRole {
    /** Names the operation, e.g. a login session id; its first value becomes the operation's display value. */
    PRIMARY,
    /** Only links traces, e.g. an authorization code handed from one step to the next. */
    ALIAS
}
