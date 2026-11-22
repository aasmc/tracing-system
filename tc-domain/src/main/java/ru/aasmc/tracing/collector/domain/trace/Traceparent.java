package ru.aasmc.tracing.collector.domain.trace;

import java.util.Optional;

/**
 * A W3C {@code traceparent} value: {@code version-traceid-parentid-flags}, e.g.
 * {@code 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01}.
 *
 * @param version the version byte (0..254)
 * @param traceId the trace id
 * @param parentSpanId the span that sent the message
 * @param flags the trace flags
 */
public record Traceparent(int version, TraceId traceId, SpanId parentSpanId, TraceFlags flags) {

    /**
     * Parses a value; never throws. Rules:
     *
     * <ul>
     *   <li>hex may be upper or lower case;
     *   <li>version {@code ff} is invalid;
     *   <li>version {@code 00} has exactly 55 characters;
     *   <li>a higher (future) version is valid if its first 55 characters parse and the 56th character, if any, is
     *       {@code '-'}: later versions may append fields;
     *   <li>an all-zero trace id or parent id is invalid.
     * </ul>
     *
     * @return the parsed value, or empty if {@code value} is null or invalid
     */
    public static Optional<Traceparent> parse(String value) {
        throw new UnsupportedOperationException();
    }
}
