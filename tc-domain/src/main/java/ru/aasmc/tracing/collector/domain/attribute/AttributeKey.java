package ru.aasmc.tracing.collector.domain.attribute;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.util.List;
import java.util.Set;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * Name of an attribute, e.g. {@code user_id} or {@code http.request.method}.
 *
 * <p>Not blank ({@code INVALID_ATTRIBUTE_KEY}); the length limit is configured in {@link AttributeLimits}. Services that
 * send spans may use any key.
 *
 * @param value the key
 */
public record AttributeKey(String value) {

    private static final List<String> RESERVED_PREFIXES = List.of("service.", "otel.", "telemetry.", "tc.");
    private static final Set<String> RESERVED_KEYS = Set.of("span.name", "status");

    public AttributeKey {
        require(value != null && !value.isBlank(), ViolationCode.INVALID_ATTRIBUTE_KEY, "attribute key is blank");
    }

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
        return !RESERVED_KEYS.contains(value) && RESERVED_PREFIXES.stream().noneMatch(value::startsWith);
    }
}
