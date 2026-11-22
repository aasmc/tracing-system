package ru.aasmc.tracing.collector.domain.trace;

/** OpenTelemetry span kind. */
public enum SpanKind {
    INTERNAL,
    SERVER,
    CLIENT,
    PRODUCER,
    CONSUMER
}
