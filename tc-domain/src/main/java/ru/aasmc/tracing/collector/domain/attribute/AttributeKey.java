package ru.aasmc.tracing.collector.domain.attribute;

/**
 * Name of an attribute, e.g. {@code user_id} or {@code http.request.method}.
 *
 * <p>Not blank and at most {@value #MAX_LENGTH} characters ({@code INVALID_ATTRIBUTE_KEY}). Services that send spans
 * may use any key.
 *
 * @param value the key
 */
public record AttributeKey(String value) {

    public static final int MAX_LENGTH = 255;

    /**
     * Whether an enrichment mapping may write this key.
     *
     * <p>Enrichment copies fields of a client's Kafka events into trace attributes, e.g. an SMS delivery report
     * becomes {@code sms.delivery.status=DELIVERED}. An admin chooses the target keys, so a careless or malicious
     * mapping could overwrite attributes the trace already has, e.g. move it to another service by writing
     * {@code service.name}. Every key is therefore allowed except these:
     *
     * <ul>
     *   <li>{@code service.*}: identifies the service that produced the span;
     *   <li>{@code otel.*} and {@code telemetry.*}: written by the OpenTelemetry SDK;
     *   <li>{@code tc.*}: reserved for attributes the collector itself adds;
     *   <li>{@code span.name} and {@code status}: search keys the collector fills from the span's name and status.
     * </ul>
     */
    public boolean allowedAsEnrichmentTarget() {
        throw new UnsupportedOperationException();
    }
}
