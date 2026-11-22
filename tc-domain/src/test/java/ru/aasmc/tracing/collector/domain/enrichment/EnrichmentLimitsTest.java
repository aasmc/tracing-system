package ru.aasmc.tracing.collector.domain.enrichment;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static ru.aasmc.tracing.collector.testmodel.domain.IdpScenario.payloadMapping;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeType;

class EnrichmentLimitsTest {

    private static final EnrichmentSourceId SOURCE_ID =
            new EnrichmentSourceId(UUID.fromString("00000000-0000-0000-0000-00000000e001"));
    private static final KafkaConnectionId CONNECTION_ID =
            new KafkaConnectionId(UUID.fromString("00000000-0000-0000-0000-00000000f001"));

    private static final EnrichmentLimits DEFAULTS = EnrichmentLimits.DEFAULT;

    @Test
    @DisplayName("by default a source name has at most 63 characters; a configured limit allows more")
    void given_nameLengths_when_checked_then_withinConfiguredLimit() {
        EnrichmentSource longName = sourceNamed("a".repeat(64));

        assertThatNoException().isThrownBy(() -> DEFAULTS.requireWithin(sourceNamed("a".repeat(63))));
        assertViolation(ViolationCode.INVALID_NAME, () -> DEFAULTS.requireWithin(longName));
        assertThatNoException().isThrownBy(() -> new EnrichmentLimits(64, 50, 128, 16, 256).requireWithin(longName));
    }

    @Test
    @DisplayName("by default a definition has at most 50 mappings; a configured limit allows more")
    void given_mappingCounts_when_checked_then_withinConfiguredLimit() {
        EnrichmentSourceDefinition fiftyOne = definitionOf(mappings(51));

        assertThatNoException().isThrownBy(() -> DEFAULTS.requireWithin(definitionOf(mappings(50))));
        assertViolation(ViolationCode.INVALID_MAPPING, () -> DEFAULTS.requireWithin(fiftyOne));
        assertThatNoException().isThrownBy(() -> new EnrichmentLimits(63, 51, 128, 16, 256).requireWithin(fiftyOne));
    }

    @Test
    @DisplayName("by default a target key has at most 128 characters")
    void given_targetKeyLengths_when_checked_then_atMost128() {
        assertThatNoException()
                .isThrownBy(() -> DEFAULTS.requireWithin(definitionOf(List.of(payloadMapping("/v", "k".repeat(128))))));
        assertViolation(
                ViolationCode.INVALID_TARGET_KEY,
                () -> DEFAULTS.requireWithin(definitionOf(List.of(payloadMapping("/v", "k".repeat(129))))));
    }

    @Test
    @DisplayName("by default a mask keeps at most 16 characters and a regex has at most 256 characters")
    void given_transformArguments_when_checked_then_withinDefaults() {
        assertThatNoException().isThrownBy(() -> DEFAULTS.requireWithin(definitionWith(new Transform.Mask(16))));
        assertViolation(
                ViolationCode.INVALID_TRANSFORM, () -> DEFAULTS.requireWithin(definitionWith(new Transform.Mask(17))));
        assertThatNoException()
                .isThrownBy(() -> DEFAULTS.requireWithin(
                        definitionWith(new Transform.RegexExtract("(" + "a".repeat(254) + ")"))));
        assertViolation(
                ViolationCode.INVALID_TRANSFORM,
                () -> DEFAULTS.requireWithin(definitionWith(new Transform.RegexExtract("(" + "a".repeat(255) + ")"))));
    }

    @Test
    @DisplayName("every limit is positive")
    void given_nonPositiveLimit_when_constructed_then_rejected() {
        assertThatIllegalArgumentException().isThrownBy(() -> new EnrichmentLimits(0, 50, 128, 16, 256));
        assertThatIllegalArgumentException().isThrownBy(() -> new EnrichmentLimits(63, 50, 128, 16, 0));
    }

    private static EnrichmentSource sourceNamed(String name) {
        return new EnrichmentSource(SOURCE_ID, name, CONNECTION_ID, "sms.reports", SourceStatus.DRAFT);
    }

    private static List<FieldMapping> mappings(int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> payloadMapping("/field" + i, "sms.field" + i))
                .toList();
    }

    private static EnrichmentSourceDefinition definitionOf(List<FieldMapping> mappings) {
        return new EnrichmentSourceDefinition(
                new TraceContextSource.TraceparentHeader(null), EnrichmentScope.TRACE, mappings);
    }

    private static EnrichmentSourceDefinition definitionWith(Transform transform) {
        FieldMapping mapping = new FieldMapping(
                FieldSource.RECORD_KEY,
                null,
                null,
                new AttributeKey("sms.value"),
                AttributeType.STRING,
                transform,
                false);
        return definitionOf(List.of(mapping));
    }
}
