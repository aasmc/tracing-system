package ru.aasmc.tracing.collector.domain.trace;

/**
 * Status of a span.
 *
 * @param code the status code
 * @param message description, usually only for errors; may be {@code null}
 */
public record SpanStatus(StatusCode code, String message) {

    public static final SpanStatus UNSET = new SpanStatus(StatusCode.UNSET, null);

    public boolean isError() {
        throw new UnsupportedOperationException();
    }
}
