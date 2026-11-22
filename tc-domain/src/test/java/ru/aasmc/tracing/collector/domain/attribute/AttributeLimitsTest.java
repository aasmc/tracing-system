package ru.aasmc.tracing.collector.domain.attribute;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.StringValue;
import ru.aasmc.tracing.collector.testmodel.domain.AttributesBuilder;

class AttributeLimitsTest {

    private static final AttributeLimits DEFAULTS = AttributeLimits.DEFAULT;

    @Test
    @DisplayName("by default a key has at most 255 characters; a configured limit allows more")
    void given_keyLengths_when_checked_then_withinConfiguredLimit() {
        Attributes longKey =
                AttributesBuilder.attributes().put("k".repeat(256), 1).build();

        assertThatNoException()
                .isThrownBy(() -> DEFAULTS.requireWithin(
                        AttributesBuilder.attributes().put("k".repeat(255), 1).build()));
        assertViolation(ViolationCode.INVALID_ATTRIBUTE_KEY, () -> DEFAULTS.requireWithin(longKey));
        assertThatNoException().isThrownBy(() -> new AttributeLimits(256, 4096, 512).requireWithin(longKey));
    }

    @Test
    @DisplayName("by default at most 512 attributes are kept; a configured limit allows more")
    void given_attributeCounts_when_checked_then_withinConfiguredLimit() {
        Attributes tooMany = attributesOfSize(513);

        assertThatNoException().isThrownBy(() -> DEFAULTS.requireWithin(attributesOfSize(512)));
        assertViolation(ViolationCode.TOO_MANY_ATTRIBUTES, () -> DEFAULTS.requireWithin(tooMany));
        assertThatNoException().isThrownBy(() -> new AttributeLimits(255, 4096, 513).requireWithin(tooMany));
    }

    @Test
    @DisplayName("a string value is cut to the configured number of bytes")
    void given_maxValueBytes_when_stringCut_then_configuredLength() {
        String fiveKiB = "x".repeat(5 * 1024);

        assertThat(StringValue.of(fiveKiB, DEFAULTS.maxValueBytes()))
                .isEqualTo(new StringValue("x".repeat(4096), true));
        assertThat(StringValue.of(fiveKiB, 8 * 1024)).isEqualTo(new StringValue(fiveKiB, false));
    }

    @Test
    @DisplayName("every limit is positive")
    void given_nonPositiveLimit_when_constructed_then_rejected() {
        assertThatIllegalArgumentException().isThrownBy(() -> new AttributeLimits(0, 4096, 512));
        assertThatIllegalArgumentException().isThrownBy(() -> new AttributeLimits(255, 0, 512));
        assertThatIllegalArgumentException().isThrownBy(() -> new AttributeLimits(255, 4096, 0));
    }

    private static Attributes attributesOfSize(int size) {
        AttributesBuilder builder = AttributesBuilder.attributes();
        for (int i = 0; i < size; i++) {
            builder.put("key-" + i, i);
        }
        return builder.build();
    }
}
