package ru.aasmc.tracing.collector.domain.enrichment;

import java.util.Optional;

/**
 * Strategy that finds the trace context in an enrichment event (05 §4.7.1, V4). The enricher reads the raw value
 * (header bytes as UTF-8, or the payload node at the pointer); {@link #interpret(String)} turns it into a context.
 */
public sealed interface TraceContextExtraction {

    /** {@code true} only for {@link TraceparentHeader}: the only strategy that carries a span id (V10). */
    boolean yieldsSpanId();

    /**
     * Interprets a raw value: a traceparent for {@link TraceparentHeader} (rules of
     * {@link ru.aasmc.tracing.collector.domain.trace.Traceparent#parse}); a bare 32-hex trace id for
     * {@link RawHeader}; either form for {@link PayloadPointer} (a traceparent there yields no span id, because the
     * scope check of V10 is static). {@link Optional#empty()} for {@code null} or invalid values.
     */
    Optional<ExtractedTraceContext> interpret(String rawValue);

    /**
     * Default strategy: W3C {@code traceparent} header; yields trace id and span id. A {@code null} or blank header
     * name becomes {@code traceparent}.
     *
     * @param headerName header to read
     */
    record TraceparentHeader(String headerName) implements TraceContextExtraction {

        public static final String DEFAULT_HEADER = "traceparent";

        @Override
        public boolean yieldsSpanId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<ExtractedTraceContext> interpret(String rawValue) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A header whose value is a bare 32-hex trace id, e.g. {@code X-Trace-Id}. Invariant: header name not blank
     * ({@code INVALID_TRACE_CONTEXT}).
     *
     * @param headerName header to read
     */
    record RawHeader(String headerName) implements TraceContextExtraction {

        @Override
        public boolean yieldsSpanId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<ExtractedTraceContext> interpret(String rawValue) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Trace id (32 hex or a traceparent string) inside the JSON payload. Invariant: pointer not null
     * ({@code INVALID_TRACE_CONTEXT}).
     *
     * @param pointer where to read
     */
    record PayloadPointer(JsonPointerExpr pointer) implements TraceContextExtraction {

        @Override
        public boolean yieldsSpanId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<ExtractedTraceContext> interpret(String rawValue) {
            throw new UnsupportedOperationException();
        }
    }
}
