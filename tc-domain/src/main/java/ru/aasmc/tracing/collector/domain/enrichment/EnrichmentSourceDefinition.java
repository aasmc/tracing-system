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
     * Turns one event into the attributes it adds:
     *
     * <ol>
     *   <li>Find the trace (and span) the event refers to with {@link #traceContextSource()}. If there is none, the
     *       result is {@link Extraction.NoTraceContext}.
     *   <li>Read every mapping in order (see {@link FieldMapping}). The first mapping that fails makes the result
     *       {@link Extraction.Rejected}, with the reason and the mapping's target key. An optional value that is missing
     *       is skipped.
     *   <li>Otherwise the result is {@link Extraction.Enriched} with the values read, for the span (SPAN scope) or for
     *       the whole trace (TRACE scope).
     * </ol>
     *
     * <p>For the SMS delivery report of the class example the result is the span named by the report's
     * {@code traceparent} header with {@code sms.delivery.status=DELIVERED} and {@code sms.provider=twilio}. Storing
     * the attributes, with the event's timestamp and Kafka position, is the caller's job.
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
