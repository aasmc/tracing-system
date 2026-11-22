package ru.aasmc.tracing.collector.domain.attribute;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.SequencedMap;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.LongValue;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.StringArrayValue;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.StringValue;
import ru.aasmc.tracing.collector.testmodel.domain.AttributesBuilder;

class AttributesTest {

    @Test
    @DisplayName("a key is not blank")
    void given_blankOrMissingKey_when_constructed_then_invalidAttributeKey() {
        assertViolation(ViolationCode.INVALID_ATTRIBUTE_KEY, () -> new AttributeKey(" "));
        assertViolation(ViolationCode.INVALID_ATTRIBUTE_KEY, () -> new AttributeKey(null));
    }

    @ParameterizedTest
    @CsvSource({
        "service.name,           false",
        "service.version,        false",
        "otel.scope.name,        false",
        "telemetry.sdk.language, false",
        "tc.source_id,           false",
        "span.name,              false",
        "status,                 false",
        "user_id,                true",
        "sms.delivery.status,    true",
        "span.kind,              true",
        "services.count,         true"
    })
    @DisplayName("enrichment may write any key except those that identify the service, the SDK or the collector")
    void given_key_when_checkedAsEnrichmentTarget_then_expectedVerdict(String key, boolean allowed) {
        assertThat(new AttributeKey(key).allowedAsEnrichmentTarget()).isEqualTo(allowed);
    }

    @Test
    @DisplayName("keeps the producer's order in an immutable copy")
    void given_mutableMap_when_constructed_then_orderedImmutableCopy() {
        SequencedMap<AttributeKey, AttributeValue> source = new LinkedHashMap<>();
        source.put(new AttributeKey("z"), new LongValue(1));
        source.put(new AttributeKey("a"), new LongValue(2));

        Attributes attributes = new Attributes(source);
        source.put(new AttributeKey("m"), new LongValue(3));

        assertThat(attributes.entries().keySet()).containsExactly(new AttributeKey("z"), new AttributeKey("a"));
        assertThat(Attributes.empty().entries()).isEmpty();
    }

    @Test
    @DisplayName("rejects null keys and values")
    void given_nullKeyOrValue_when_constructed_then_rejected() {
        SequencedMap<AttributeKey, AttributeValue> nullKey = new LinkedHashMap<>();
        nullKey.put(null, new LongValue(1));
        SequencedMap<AttributeKey, AttributeValue> nullValue = new LinkedHashMap<>();
        nullValue.put(new AttributeKey("k"), null);

        assertViolation(ViolationCode.INVALID_ATTRIBUTE_KEY, () -> new Attributes(nullKey));
        assertViolation(ViolationCode.INVALID_ATTRIBUTE_TYPE, () -> new Attributes(nullValue));
    }

    @Test
    @DisplayName("knows whether any string or string-array value was cut")
    void given_values_when_anyValueTruncated_then_trueOnlyIfOneWasCut() {
        Attributes intact = AttributesBuilder.attributes()
                .put("a", "short")
                .put("b", new StringArrayValue(List.of("x"), false))
                .build();
        Attributes cutString = AttributesBuilder.attributes()
                .put("a", new StringValue("x", true))
                .build();
        Attributes cutArray = AttributesBuilder.attributes()
                .put("b", new StringArrayValue(List.of("x"), true))
                .build();

        assertThat(intact.anyValueTruncated()).isFalse();
        assertThat(cutString.anyValueTruncated()).isTrue();
        assertThat(cutArray.anyValueTruncated()).isTrue();
    }
}
