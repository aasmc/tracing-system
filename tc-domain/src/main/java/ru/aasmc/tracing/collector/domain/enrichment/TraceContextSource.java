package ru.aasmc.tracing.collector.domain.enrichment;

import java.util.Optional;

/** Where in an enrichment event the trace (and possibly span) it refers to is found. */
public sealed interface TraceContextSource {

    /** Whether this source can name a span, which a SPAN-scoped enrichment source needs. */
    boolean providesSpanId();

    /**
     * Parses the raw value read from the event (a header as UTF-8 text, or the payload value at the pointer).
     *
     * @return the context, or empty if {@code rawValue} is null or invalid
     */
    Optional<TraceContext> parse(String rawValue);

    /**
     * The default: a W3C {@code traceparent} header (see {@link ru.aasmc.tracing.collector.domain.trace.Traceparent})
     * with trace id and span id. A {@code null} or blank header name means {@value #DEFAULT_HEADER}.
     *
     * @param headerName the header to read
     */
    record TraceparentHeader(String headerName) implements TraceContextSource {

        public static final String DEFAULT_HEADER = "traceparent";

        @Override
        public boolean providesSpanId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<TraceContext> parse(String rawValue) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A header holding a bare 32-hex trace id, e.g. {@code X-Trace-Id}.
     *
     * @param headerName the header to read, not blank ({@code INVALID_TRACE_CONTEXT})
     */
    record TraceIdHeader(String headerName) implements TraceContextSource {

        @Override
        public boolean providesSpanId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<TraceContext> parse(String rawValue) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * A payload field holding a bare trace id or a {@code traceparent}. Only the trace id is used: whether the field
     * holds a span id is unknown until events arrive, so a source using it cannot be SPAN-scoped.
     *
     * @param pointer where the field is, not null ({@code INVALID_TRACE_CONTEXT})
     */
    record PayloadField(JsonPointer pointer) implements TraceContextSource {

        @Override
        public boolean providesSpanId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<TraceContext> parse(String rawValue) {
            throw new UnsupportedOperationException();
        }
    }
}
