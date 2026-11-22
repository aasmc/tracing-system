package ru.aasmc.tracing.collector.domain.enrichment;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.payloadMapping;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeType;
import ru.aasmc.tracing.collector.domain.enrichment.TraceContextSource.PayloadField;
import ru.aasmc.tracing.collector.domain.enrichment.TraceContextSource.TraceIdHeader;
import ru.aasmc.tracing.collector.domain.enrichment.TraceContextSource.TraceparentHeader;
import ru.aasmc.tracing.collector.testmodel.domain.IdpScenario;

class EnrichmentSourceDefinitionTest {

    private static final TraceContextSource TRACEPARENT = new TraceparentHeader(null);
    private static final FieldMapping STATUS = payloadMapping("/status", "sms.delivery.status");

    @Test
    @DisplayName("target keys are unique")
    void given_sameTargetTwice_when_constructed_then_duplicateTargetKey() {
        FieldMapping sameTarget = payloadMapping("/state", "sms.delivery.status");

        assertViolation(
                ViolationCode.DUPLICATE_TARGET_KEY,
                () -> new EnrichmentSourceDefinition(TRACEPARENT, EnrichmentScope.TRACE, List.of(STATUS, sameTarget)));
    }

    @Test
    @DisplayName("a definition has at least one mapping, kept as an immutable copy")
    void given_mappings_when_constructed_then_atLeastOneInImmutableCopy() {
        List<FieldMapping> source = new ArrayList<>(List.of(STATUS));
        EnrichmentSourceDefinition definition =
                new EnrichmentSourceDefinition(TRACEPARENT, EnrichmentScope.TRACE, source);
        source.removeLast();

        assertThat(definition.fieldMappings()).containsExactly(STATUS);
        assertViolation(
                ViolationCode.INVALID_MAPPING,
                () -> new EnrichmentSourceDefinition(TRACEPARENT, EnrichmentScope.TRACE, List.of()));
    }

    @Test
    @DisplayName("span scope needs a trace-context source that names a span")
    void given_spanScope_when_sourceNamesNoSpan_then_invalidScope() {
        assertThat(IdpScenario.smsDefinition().scope()).isEqualTo(EnrichmentScope.SPAN);
        assertViolation(
                ViolationCode.INVALID_SCOPE,
                () -> new EnrichmentSourceDefinition(
                        new TraceIdHeader("X-Trace-Id"), EnrichmentScope.SPAN, List.of(STATUS)));
        assertViolation(
                ViolationCode.INVALID_SCOPE,
                () -> new EnrichmentSourceDefinition(
                        new PayloadField(new JsonPointer("/trace")), EnrichmentScope.SPAN, List.of(STATUS)));
    }

    @Test
    @DisplayName("trace-context source and scope are required")
    void given_missingSourceOrScope_when_constructed_then_rejected() {
        assertViolation(
                ViolationCode.INVALID_TRACE_CONTEXT,
                () -> new EnrichmentSourceDefinition(null, EnrichmentScope.TRACE, List.of(STATUS)));
        assertViolation(
                ViolationCode.INVALID_SCOPE, () -> new EnrichmentSourceDefinition(TRACEPARENT, null, List.of(STATUS)));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "service.name",
                "service.version",
                "otel.status_code",
                "telemetry.sdk.name",
                "tc.source_id",
                "span.name",
                "status"
            })
    @DisplayName("a mapping may not overwrite keys of the service, the SDK or the collector")
    void given_reservedTarget_when_mapped_then_reservedKey(String target) {
        assertViolation(ViolationCode.RESERVED_KEY, () -> payloadMapping("/value", target));
    }

    @ParameterizedTest
    @ValueSource(strings = {"Sms.status", "1sms", "_sms", "sms status", "sms/status"})
    @DisplayName("a target key is lower-case letters, digits, '_', '.' or '-', starting with a letter")
    void given_malformedTarget_when_mapped_then_invalidTargetKey(String target) {
        assertViolation(ViolationCode.INVALID_TARGET_KEY, () -> payloadMapping("/value", target));
    }

    @Test
    @DisplayName("a payload mapping needs a pointer and a header mapping a header name")
    void given_missingLocation_when_mapped_then_invalidMapping() {
        assertViolation(
                ViolationCode.INVALID_MAPPING,
                () -> mapping(FieldSource.PAYLOAD, null, null, AttributeType.STRING, new Transform.None()));
        assertViolation(
                ViolationCode.INVALID_MAPPING,
                () -> mapping(FieldSource.HEADER, null, " ", AttributeType.STRING, new Transform.None()));
    }

    @Test
    @DisplayName("a mapping writes one value, so array types are rejected")
    void given_arrayType_when_mapped_then_invalidMapping() {
        assertViolation(
                ViolationCode.INVALID_MAPPING,
                () -> mapping(FieldSource.RECORD_KEY, null, null, AttributeType.STRING_ARRAY, null));
    }

    @Test
    @DisplayName("the record timestamp maps to epoch millis (LONG) or ISO-8601 text (STRING) only")
    void given_recordTimestamp_when_mapped_then_longOrStringOnly() {
        assertThat(mapping(FieldSource.RECORD_TIMESTAMP, null, null, AttributeType.LONG, null)
                        .valueType())
                .isEqualTo(AttributeType.LONG);
        assertViolation(
                ViolationCode.INVALID_MAPPING,
                () -> mapping(FieldSource.RECORD_TIMESTAMP, null, null, AttributeType.BOOLEAN, null));
    }

    @Test
    @DisplayName("a transform works on strings only, and a missing transform means none")
    void given_transform_when_mapped_then_stringOnlyAndNoneByDefault() {
        FieldMapping withoutTransform = mapping(FieldSource.RECORD_KEY, null, null, AttributeType.STRING, null);

        assertThat(withoutTransform.transform()).isEqualTo(new Transform.None());
        assertViolation(
                ViolationCode.INVALID_TRANSFORM,
                () -> mapping(FieldSource.RECORD_KEY, null, null, AttributeType.LONG, new Transform.Lowercase()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "/", "/a/b~1c", "/a~0b", "/0", "/with space"})
    @DisplayName("accepts JSON Pointer syntax")
    void given_validPointer_when_constructed_then_accepted(String pointer) {
        assertThat(new JsonPointer(pointer).value()).isEqualTo(pointer);
    }

    @ParameterizedTest
    @ValueSource(strings = {"a/b", "/a~2", "/a~", "#/a"})
    @DisplayName("rejects text that is not a JSON Pointer")
    void given_invalidPointer_when_constructed_then_invalidJsonPointer(String pointer) {
        assertViolation(ViolationCode.INVALID_JSON_POINTER, () -> new JsonPointer(pointer));
    }

    @Test
    @DisplayName("a mask cannot keep a negative number of characters")
    void given_negativeMaskLength_when_constructed_then_invalidTransform() {
        assertThat(new Transform.Mask(0).keepLastChars()).isZero();
        assertViolation(ViolationCode.INVALID_TRANSFORM, () -> new Transform.Mask(-1));
    }

    @Test
    @DisplayName("a regex extract compiles as RE2 and has a capturing group")
    void given_regexes_when_constructed_then_onlyValidRe2WithGroup() {
        assertThat(new Transform.RegexExtract("code=(\\w+)").regex()).isEqualTo("code=(\\w+)");
        assertViolation(ViolationCode.INVALID_TRANSFORM, () -> new Transform.RegexExtract("(unclosed"));
        assertViolation(ViolationCode.INVALID_TRANSFORM, () -> new Transform.RegexExtract("no-group"));
        assertViolation(ViolationCode.INVALID_TRANSFORM, () -> new Transform.RegexExtract("(a)\\1"));
    }

    private static FieldMapping mapping(
            FieldSource source, JsonPointer pointer, String header, AttributeType type, Transform transform) {
        return new FieldMapping(source, pointer, header, new AttributeKey("sms.value"), type, transform, false);
    }

    @Test
    @DisplayName("a mapping needs a source, a type and a target key, and a header mapping a header name")
    void given_missingParts_when_mapped_then_rejected() {
        AttributeKey target = new AttributeKey("sms.value");

        assertViolation(
                ViolationCode.INVALID_MAPPING,
                () -> new FieldMapping(null, null, null, target, AttributeType.STRING, null, false));
        assertViolation(
                ViolationCode.INVALID_MAPPING,
                () -> new FieldMapping(FieldSource.RECORD_KEY, null, null, target, null, null, false));
        assertViolation(
                ViolationCode.INVALID_MAPPING,
                () -> new FieldMapping(FieldSource.HEADER, null, null, target, AttributeType.STRING, null, false));
        assertViolation(
                ViolationCode.INVALID_TARGET_KEY,
                () -> new FieldMapping(FieldSource.RECORD_KEY, null, null, null, AttributeType.STRING, null, false));
    }
}
