package ru.aasmc.tracing.collector.domain.enrichment;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.SequencedMap;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue;
import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/**
 * What an enrichment source does with each event: find the trace, then map fields to attributes.
 *
 * <p>Example: an SMS gateway reports deliveries on its own Kafka topic. Each report carries the {@code traceparent} of
 * the span that sent the SMS, and a payload {@code {"status":"DELIVERED","provider":"twilio"}}. The definition
 * {@code TraceparentHeader("traceparent")}, scope SPAN, mappings {@code PAYLOAD /status → sms.delivery.status} and
 * {@code PAYLOAD /provider → sms.provider} turns each report into two attributes of that span (see {@link #extract}).
 *
 * <p>Rules: {@code traceContextSource} and {@code scope} are set; SPAN scope needs a trace-context source that
 * {@link TraceContextSource#providesSpanId() yields a span id} ({@code INVALID_SCOPE}); at least one mapping
 * ({@code INVALID_MAPPING}; the maximum is configured in {@link EnrichmentLimits}) with distinct target keys
 * ({@code DUPLICATE_TARGET_KEY}); the list is an immutable copy.
 *
 * @param traceContextSource where the trace is found in an event
 * @param scope trace or span
 * @param fieldMappings the mappings, in order
 */
public record EnrichmentSourceDefinition(
        TraceContextSource traceContextSource, EnrichmentScope scope, List<FieldMapping> fieldMappings) {

    public EnrichmentSourceDefinition {
        require(traceContextSource != null, ViolationCode.INVALID_TRACE_CONTEXT, "no trace-context source");
        require(scope != null, ViolationCode.INVALID_SCOPE, "no scope");
        require(
                scope != EnrichmentScope.SPAN || traceContextSource.providesSpanId(),
                ViolationCode.INVALID_SCOPE,
                "SPAN scope needs a trace-context source that names a span");
        require(
                fieldMappings != null && !fieldMappings.isEmpty(),
                ViolationCode.INVALID_MAPPING,
                "a definition has at least one mapping");
        fieldMappings = List.copyOf(fieldMappings);
        long distinctTargets =
                fieldMappings.stream().map(FieldMapping::targetKey).distinct().count();
        require(
                distinctTargets == fieldMappings.size(),
                ViolationCode.DUPLICATE_TARGET_KEY,
                "two mappings write the same attribute");
    }

    /**
     * Applies this definition to one event:
     *
     * <ol>
     *   <li>find the trace (and span) with {@link #traceContextSource()}; none →
     *       {@link Extraction.NoTraceContext};
     *   <li>read every mapping in order ({@link FieldMapping} explains one read); the first failure →
     *       {@link Extraction.Rejected} with its reason and the mapping's target key; an optional value that is missing
     *       is skipped;
     *   <li>otherwise → {@link Extraction.Enriched} with the values read, for the span (SPAN scope) or the whole trace
     *       (TRACE scope).
     * </ol>
     *
     * <p>For the SMS report of the class example the result is {@code Enriched(trace T2, span 5f3a9b2c1d0e4f68,
     * {sms.delivery.status=DELIVERED, sms.provider=twilio})}. The event's timestamp, Kafka coordinates and storage are
     * the caller's concern.
     *
     * @param maxValueBytes string values are cut to this many UTF-8 bytes (the configured attribute value limit)
     */
    public Extraction extract(EnrichmentEvent event, int maxValueBytes) {
        Optional<TraceContext> context = traceContextSource.extract(event);
        if (context.isEmpty()) {
            return new Extraction.NoTraceContext();
        }
        SequencedMap<AttributeKey, AttributeValue> values = new LinkedHashMap<>();
        for (FieldMapping mapping : fieldMappings) {
            switch (mapping.read(event, maxValueBytes)) {
                case FieldReading.Found<AttributeValue>(AttributeValue value) -> values.put(mapping.targetKey(), value);
                case FieldReading.Missing<AttributeValue> _ -> {}
                case FieldReading.Failed<AttributeValue>(ExtractionFailure reason) -> {
                    return new Extraction.Rejected(reason, mapping.targetKey());
                }
            }
        }
        TraceContext target = scope == EnrichmentScope.SPAN
                ? context.get()
                : new TraceContext(context.get().traceId(), null);
        return new Extraction.Enriched(target, new Attributes(values));
    }
}
