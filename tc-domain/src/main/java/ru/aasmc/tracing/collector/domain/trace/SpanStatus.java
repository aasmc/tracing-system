package ru.aasmc.tracing.collector.domain.trace;

/**
 * Span status (05 §3.6).
 *
 * <p>Invariants: {@code code} is not null. An empty {@code message} is normalised to {@code null} (the starter sends
 * {@code ""} for "no description", stored as SQL NULL).
 *
 * @param code the status code
 * @param message the description, or {@code null}
 */
public record SpanStatus(StatusCode code, String message) {

    /** Status of spans whose producer did not set one. */
    public static final SpanStatus UNSET = new SpanStatus(StatusCode.UNSET, null);

    public boolean isError() {
        throw new UnsupportedOperationException();
    }
}
