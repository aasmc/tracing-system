package ru.aasmc.tracing.collector.domain.enrichment;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeType;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.BoolValue;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.DoubleValue;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.LongValue;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.StringValue;
import ru.aasmc.tracing.collector.domain.enrichment.JsonScalar.BooleanScalar;
import ru.aasmc.tracing.collector.domain.enrichment.JsonScalar.NumberScalar;
import ru.aasmc.tracing.collector.domain.enrichment.JsonScalar.StringScalar;
import ru.aasmc.tracing.collector.domain.enrichment.TraceContextSource.TraceparentHeader;
import ru.aasmc.tracing.collector.testmodel.domain.AttributesBuilder;
import ru.aasmc.tracing.collector.testmodel.domain.DomainIds;
import ru.aasmc.tracing.collector.testmodel.domain.IdpScenario;
import ru.aasmc.tracing.collector.testmodel.domain.InMemoryEnrichmentEvent;

class EnrichmentExtractionTest {

    private static final int MAX_VALUE_BYTES = 4096;
    private static final AttributeKey TARGET = new AttributeKey("sms.value");
    private static final TraceContext T2_TRACE = new TraceContext(IdpScenario.T2, null);

    @Test
    @DisplayName("an SMS delivery report adds its status and provider to the span that sent the SMS")
    void given_smsDeliveryReport_when_extracted_then_attributesOfSmsSpan() {
        Extraction extraction = IdpScenario.smsDefinition().extract(IdpScenario.smsDeliveryReport(), MAX_VALUE_BYTES);

        assertThat(extraction)
                .isEqualTo(new Extraction.Enriched(
                        new TraceContext(IdpScenario.T2, DomainIds.spanId("5f3a9b2c1d0e4f68")),
                        AttributesBuilder.attributes()
                                .put("sms.delivery.status", "DELIVERED")
                                .put("sms.provider", "twilio")
                                .build()));
    }

    @Test
    @DisplayName("with trace scope the attributes belong to the whole trace, not to the span")
    void given_traceScope_when_extracted_then_noSpan() {
        EnrichmentSourceDefinition traceScoped = new EnrichmentSourceDefinition(
                new TraceparentHeader(null),
                EnrichmentScope.TRACE,
                IdpScenario.smsDefinition().fieldMappings());

        Extraction extraction = traceScoped.extract(IdpScenario.smsDeliveryReport(), MAX_VALUE_BYTES);

        assertThat(extraction)
                .isInstanceOfSatisfying(
                        Extraction.Enriched.class,
                        enriched -> assertThat(enriched.target()).isEqualTo(T2_TRACE));
    }

    @Test
    @DisplayName("an event without trace context names no trace")
    void given_noTraceparent_when_extracted_then_noTraceContext() {
        InMemoryEnrichmentEvent withoutTraceparent =
                InMemoryEnrichmentEvent.anEvent().withPayload("/status", "DELIVERED");

        assertThat(IdpScenario.smsDefinition().extract(withoutTraceparent, MAX_VALUE_BYTES))
                .isEqualTo(new Extraction.NoTraceContext());
    }

    @Test
    @DisplayName("a missing optional value is skipped, a missing required value rejects the event")
    void given_missingValue_when_extracted_then_skippedOrRejected() {
        InMemoryEnrichmentEvent noValue = eventOfT2();

        assertThat(extract(mapping(FieldSource.PAYLOAD, "/absent", null, AttributeType.STRING, null, false), noValue))
                .isEqualTo(new Extraction.Enriched(T2_TRACE, AttributesBuilder.empty()));
        assertThat(extract(mapping(FieldSource.PAYLOAD, "/absent", null, AttributeType.STRING, null, true), noValue))
                .isEqualTo(new Extraction.Rejected(ExtractionFailure.MISSING_REQUIRED_FIELD, TARGET));
    }

    @Test
    @DisplayName("values are read from a header, the record key and the record timestamp")
    void given_otherLocations_when_extracted_then_valuesRead() {
        InMemoryEnrichmentEvent event =
                eventOfT2().withHeader("X-Status", "SENT").withKey("msg-1");

        assertThat(valueOf(mapping(FieldSource.HEADER, null, "X-Status", AttributeType.STRING, null, true), event))
                .isEqualTo(new StringValue("SENT", false));
        assertThat(valueOf(mapping(FieldSource.RECORD_KEY, null, null, AttributeType.STRING, null, true), event))
                .isEqualTo(new StringValue("msg-1", false));
        assertThat(valueOf(mapping(FieldSource.RECORD_TIMESTAMP, null, null, AttributeType.LONG, null, true), event))
                .isEqualTo(new LongValue(InMemoryEnrichmentEvent.TIMESTAMP.toEpochMilli()));
        assertThat(valueOf(mapping(FieldSource.RECORD_TIMESTAMP, null, null, AttributeType.STRING, null, true), event))
                .isEqualTo(new StringValue("2026-11-21T09:15:40Z", false));
    }

    static Stream<Arguments> conversions() {
        return Stream.of(
                Arguments.of(
                        "text as STRING", AttributeType.STRING, new StringScalar("x"), new StringValue("x", false)),
                Arguments.of("number as STRING", AttributeType.STRING, number("42"), new StringValue("42", false)),
                Arguments.of(
                        "number with an exponent as STRING keeps the exponent",
                        AttributeType.STRING,
                        number("1e3"),
                        new StringValue("1E+3", false)),
                Arguments.of(
                        "number with a huge exponent as STRING is not expanded into its digits",
                        AttributeType.STRING,
                        number("1e2000000000"),
                        new StringValue("1E+2000000000", false)),
                Arguments.of(
                        "boolean as STRING",
                        AttributeType.STRING,
                        new BooleanScalar(true),
                        new StringValue("true", false)),
                Arguments.of("boolean as BOOLEAN", AttributeType.BOOLEAN, new BooleanScalar(true), new BoolValue(true)),
                Arguments.of(
                        "text false as BOOLEAN",
                        AttributeType.BOOLEAN,
                        new StringScalar("false"),
                        new BoolValue(false)),
                Arguments.of("number as LONG", AttributeType.LONG, number("42"), new LongValue(42)),
                Arguments.of("text as LONG", AttributeType.LONG, new StringScalar("42"), new LongValue(42)),
                Arguments.of("number as DOUBLE", AttributeType.DOUBLE, number("4.5"), new DoubleValue(4.5)),
                Arguments.of(
                        "exponent text as DOUBLE",
                        AttributeType.DOUBLE,
                        new StringScalar("1e3"),
                        new DoubleValue(1000)),
                Arguments.of("whole number with .0 as LONG", AttributeType.LONG, number("42.0"), new LongValue(42)),
                Arguments.of(
                        "number as DOUBLE keeps the fraction",
                        AttributeType.DOUBLE,
                        number("0.1"),
                        new DoubleValue(0.1)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("conversions")
    @DisplayName("a JSON value, or text of the right form, is converted to the mapping's type")
    void given_rawValue_when_extracted_then_convertedToType(
            String name, AttributeType type, JsonScalar raw, AttributeValue expected) {
        assertThat(valueOf(payloadMapping(type, null), eventOfT2().withPayload("/value", raw)))
                .isEqualTo(expected);
    }

    static Stream<Arguments> mismatches() {
        return Stream.of(
                Arguments.of("text yes as BOOLEAN", AttributeType.BOOLEAN, new StringScalar("yes")),
                Arguments.of("fraction as LONG", AttributeType.LONG, number("4.5")),
                Arguments.of("text as LONG", AttributeType.LONG, new StringScalar("abc")),
                Arguments.of("number above the long range", AttributeType.LONG, number("9223372036854775808")),
                Arguments.of("number with a huge exponent as LONG", AttributeType.LONG, number("1e2000000000")),
                Arguments.of("boolean as LONG", AttributeType.LONG, new BooleanScalar(true)),
                Arguments.of("text as DOUBLE", AttributeType.DOUBLE, new StringScalar("x")),
                Arguments.of("number as BOOLEAN", AttributeType.BOOLEAN, number("1")),
                Arguments.of("text with spaces as LONG", AttributeType.LONG, new StringScalar(" 42")),
                Arguments.of("number too large for a double", AttributeType.DOUBLE, number("1e400")));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("mismatches")
    @DisplayName("a value that does not fit the mapping's type rejects the event")
    void given_valueOfWrongType_when_extracted_then_typeMismatch(String name, AttributeType type, JsonScalar raw) {
        assertThat(extract(payloadMapping(type, null), eventOfT2().withPayload("/value", raw)))
                .isEqualTo(new Extraction.Rejected(ExtractionFailure.TYPE_MISMATCH, TARGET));
    }

    static Stream<Arguments> transforms() {
        return Stream.of(
                Arguments.of("none", new Transform.None(), "AbC", "AbC"),
                Arguments.of("lowercase", new Transform.Lowercase(), "AbC", "abc"),
                Arguments.of("uppercase", new Transform.Uppercase(), "AbC", "ABC"),
                Arguments.of("trim", new Transform.Trim(), "  a b  ", "a b"),
                Arguments.of(
                        "sha256",
                        new Transform.Sha256(),
                        "u-42",
                        "16ec525b74b75626eb93ecad8acc749885fcf4e83c95b139342769a4ab1828a3"),
                Arguments.of("mask keeps the last 4", new Transform.Mask(4), "+79991234567", "********4567"),
                Arguments.of("mask of a short value", new Transform.Mask(4), "123", "123"),
                Arguments.of(
                        "regex keeps the first group",
                        new Transform.RegexExtract("code=(\\w+)"),
                        "x code=abc y",
                        "abc"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("transforms")
    @DisplayName("a transform changes a string value before it is stored")
    void given_transform_when_extracted_then_transformedValue(
            String name, Transform transform, String raw, String expected) {
        assertThat(valueOf(
                        payloadMapping(AttributeType.STRING, transform),
                        eventOfT2().withPayload("/value", raw)))
                .isEqualTo(new StringValue(expected, false));
    }

    @Test
    @DisplayName("a regular expression that does not match rejects the event")
    void given_regexWithoutMatch_when_extracted_then_transformFailed() {
        FieldMapping mapping = payloadMapping(AttributeType.STRING, new Transform.RegexExtract("code=(\\w+)"));

        assertThat(extract(mapping, eventOfT2().withPayload("/value", "no code here")))
                .isEqualTo(new Extraction.Rejected(ExtractionFailure.TRANSFORM_FAILED, TARGET));
    }

    @Test
    @DisplayName("a string value is cut to the configured size and flagged")
    void given_longString_when_extracted_then_cutToLimit() {
        EnrichmentSourceDefinition definition = definitionOf(payloadMapping(AttributeType.STRING, null));

        Extraction extraction = definition.extract(eventOfT2().withPayload("/value", "abcdef"), 3);

        assertThat(extraction)
                .isInstanceOfSatisfying(
                        Extraction.Enriched.class,
                        enriched -> assertThat(enriched.attributes().entries().get(TARGET))
                                .isEqualTo(new StringValue("abc", true)));
    }

    private static InMemoryEnrichmentEvent eventOfT2() {
        return InMemoryEnrichmentEvent.anEvent().withHeader("traceparent", IdpScenario.SMS_TRACEPARENT);
    }

    private static NumberScalar number(String text) {
        return new NumberScalar(new BigDecimal(text));
    }

    private static FieldMapping payloadMapping(AttributeType type, Transform transform) {
        return mapping(FieldSource.PAYLOAD, "/value", null, type, transform, true);
    }

    private static FieldMapping mapping(
            FieldSource source,
            String pointer,
            String header,
            AttributeType type,
            Transform transform,
            boolean required) {
        JsonPointer jsonPointer = pointer == null ? null : new JsonPointer(pointer);
        return new FieldMapping(source, jsonPointer, header, TARGET, type, transform, required);
    }

    private static EnrichmentSourceDefinition definitionOf(FieldMapping mapping) {
        return new EnrichmentSourceDefinition(new TraceparentHeader(null), EnrichmentScope.TRACE, List.of(mapping));
    }

    private static Extraction extract(FieldMapping mapping, InMemoryEnrichmentEvent event) {
        return definitionOf(mapping).extract(event, MAX_VALUE_BYTES);
    }

    /** The value the single mapping produced. */
    private static AttributeValue valueOf(FieldMapping mapping, InMemoryEnrichmentEvent event) {
        Extraction extraction = extract(mapping, event);
        assertThat(extraction).isInstanceOf(Extraction.Enriched.class);
        return ((Extraction.Enriched) extraction).attributes().entries().get(TARGET);
    }

    @Test
    @DisplayName("""
            Given a regular expression whose capturing group can take no part in the match, (a)|b,
            when the value is b (it matches, but the group is empty),
            then the transform gives no value and the event is rejected with TRANSFORM_FAILED""")
    void given_regexGroupNotParticipating_when_extracted_then_transformFailed() {
        FieldMapping mapping = payloadMapping(AttributeType.STRING, new Transform.RegexExtract("(a)|b"));

        assertThat(extract(mapping, eventOfT2().withPayload("/value", "b")))
                .isEqualTo(new Extraction.Rejected(ExtractionFailure.TRANSFORM_FAILED, TARGET));
    }

    @Test
    @DisplayName("""
            Given optional header and record-key mappings,
            when the event has no such header and no key,
            then both are skipped and the event still enriches the trace, with no attributes""")
    void given_missingOptionalHeaderAndKey_when_extracted_then_skipped() {
        FieldMapping header =
                new FieldMapping(FieldSource.HEADER, null, "X-Status", TARGET, AttributeType.STRING, null, false);
        FieldMapping key = new FieldMapping(
                FieldSource.RECORD_KEY, null, null, new AttributeKey("sms.key"), AttributeType.STRING, null, false);
        EnrichmentSourceDefinition definition = new EnrichmentSourceDefinition(
                new TraceparentHeader(null), EnrichmentScope.TRACE, List.of(header, key));

        assertThat(definition.extract(eventOfT2(), MAX_VALUE_BYTES))
                .isEqualTo(new Extraction.Enriched(T2_TRACE, AttributesBuilder.empty()));
    }

    @Test
    @DisplayName("""
            Given two required mappings that both fail,
            when the event is extracted,
            then it is rejected for the first mapping in definition order""")
    void given_twoFailingMappings_when_extracted_then_firstFailureReported() {
        FieldMapping first = new FieldMapping(
                FieldSource.PAYLOAD,
                new JsonPointer("/first"),
                null,
                new AttributeKey("sms.first"),
                AttributeType.STRING,
                null,
                true);
        FieldMapping second = new FieldMapping(
                FieldSource.PAYLOAD,
                new JsonPointer("/second"),
                null,
                new AttributeKey("sms.second"),
                AttributeType.STRING,
                null,
                true);
        EnrichmentSourceDefinition definition = new EnrichmentSourceDefinition(
                new TraceparentHeader(null), EnrichmentScope.TRACE, List.of(first, second));

        assertThat(definition.extract(eventOfT2(), MAX_VALUE_BYTES))
                .isEqualTo(new Extraction.Rejected(
                        ExtractionFailure.MISSING_REQUIRED_FIELD, new AttributeKey("sms.first")));
    }
}
