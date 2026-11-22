package ru.aasmc.tracing.collector.domain.enrichment;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.util.Optional;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.trace.TraceId;
import ru.aasmc.tracing.collector.domain.trace.Traceparent;

/**
 * Where in an enrichment event the trace (and possibly span) it refers to is found, and how that value is read: a
 * {@code traceparent} header, a header with a bare trace id, or a payload field.
 */
public sealed interface TraceContextSource {

    /** Whether this source can name a span, which a SPAN-scoped enrichment source needs. */
    boolean providesSpanId();

    /**
     * Reads the trace context from the place this source names.
     *
     * @return the context, or empty if the event has no value there or the value is not a valid trace context
     */
    Optional<TraceContext> extract(EnrichmentEvent event);

    /**
     * The default: a W3C {@code traceparent} header (see {@link ru.aasmc.tracing.collector.domain.trace.Traceparent})
     * with trace id and span id. A {@code null} or blank header name means {@value #DEFAULT_HEADER}.
     *
     * @param headerName the header to read
     */
    record TraceparentHeader(String headerName) implements TraceContextSource {

        public static final String DEFAULT_HEADER = "traceparent";

        public TraceparentHeader {
            headerName = headerName == null || headerName.isBlank() ? DEFAULT_HEADER : headerName;
        }

        @Override
        public boolean providesSpanId() {
            return true;
        }

        @Override
        public Optional<TraceContext> extract(EnrichmentEvent event) {
            return event.header(headerName)
                    .flatMap(Traceparent::parse)
                    .map(traceparent -> new TraceContext(traceparent.traceId(), traceparent.parentSpanId()));
        }
    }

    /**
     * A header holding a bare 32-hex trace id, e.g. {@code X-Trace-Id}.
     *
     * @param headerName the header to read, not blank ({@code INVALID_TRACE_CONTEXT})
     */
    record TraceIdHeader(String headerName) implements TraceContextSource {

        public TraceIdHeader {
            require(
                    headerName != null && !headerName.isBlank(),
                    ViolationCode.INVALID_TRACE_CONTEXT,
                    "trace-id header without a name");
        }

        @Override
        public boolean providesSpanId() {
            return false;
        }

        @Override
        public Optional<TraceContext> extract(EnrichmentEvent event) {
            return event.header(headerName).flatMap(TraceId::tryParse).map(traceId -> new TraceContext(traceId, null));
        }
    }

    /**
     * A payload field holding a bare trace id or a {@code traceparent}. Only the trace id is used: whether the field
     * holds a span id is unknown until events arrive, so a source using it cannot be SPAN-scoped.
     *
     * @param pointer where the field is, not null ({@code INVALID_TRACE_CONTEXT})
     */
    record PayloadField(JsonPointer pointer) implements TraceContextSource {

        public PayloadField {
            require(pointer != null, ViolationCode.INVALID_TRACE_CONTEXT, "payload field without a pointer");
        }

        @Override
        public boolean providesSpanId() {
            return false;
        }

        @Override
        public Optional<TraceContext> extract(EnrichmentEvent event) {
            return event.payloadValue(pointer)
                    .flatMap(value -> value instanceof JsonScalar.StringScalar(String text)
                            ? Optional.of(text)
                            : Optional.<String>empty())
                    .flatMap(text -> TraceId.tryParse(text)
                            .or(() -> Traceparent.parse(text).map(Traceparent::traceId)))
                    .map(traceId -> new TraceContext(traceId, null));
        }
    }
}
