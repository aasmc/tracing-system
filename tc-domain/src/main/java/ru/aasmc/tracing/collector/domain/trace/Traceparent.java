package ru.aasmc.tracing.collector.domain.trace;

import java.util.Optional;

/**
 * A parsed W3C {@code traceparent} value {@code version-traceid-parentid-flags} (W3C Trace Context Level 1 §3.2),
 * used by enrichment trace-context extraction (05 §4.7.1, 06 UC-06).
 *
 * <p>Parsing rules ({@link #parse}): lower- or upper-case hex is accepted and normalised; version {@code ff} is
 * invalid; version {@code 00} must have exactly 55 characters; a higher (future) version is accepted if its first 55
 * characters parse and character 56, if present, is {@code '-'}; an all-zero trace id or parent id is invalid.
 *
 * @param version the version byte (0..254)
 * @param traceId the trace id
 * @param parentSpanId the parent (caller) span id
 * @param flags the trace flags
 */
public record Traceparent(int version, TraceId traceId, SpanId parentSpanId, TraceFlags flags) {

    /** Length of a version-00 value. */
    public static final int V00_LENGTH = 55;

    /** Parses a header or payload value; {@link Optional#empty()} for anything invalid, never an exception. */
    public static Optional<Traceparent> parse(String value) {
        throw new UnsupportedOperationException();
    }
}
