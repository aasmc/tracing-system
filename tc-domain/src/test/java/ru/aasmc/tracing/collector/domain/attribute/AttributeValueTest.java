package ru.aasmc.tracing.collector.domain.attribute;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.LongArrayValue;
import ru.aasmc.tracing.collector.testmodel.domain.AttributeMatrix.AttributeCase;
import ru.aasmc.tracing.collector.testmodel.domain.AttributeMatrix.Expectation;

class AttributeValueTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("ru.aasmc.tracing.collector.testmodel.domain.AttributeMatrix#valid")
    @DisplayName("each attribute type keeps its value and gives its type, index values, number and truncation")
    void given_validValue_when_constructed_then_expectedTypedValue(AttributeCase attributeCase) {
        Expectation.Valid expected = (Expectation.Valid) attributeCase.expected();

        AttributeValue value = attributeCase.construct().get();

        assertThat(value).isEqualTo(expected.value());
        assertThat(value.type()).isEqualTo(expected.type());
        assertThat(value.indexValues()).isEqualTo(expected.indexValues());
        assertThat(value.numericValue()).isEqualTo(expected.numericValue());
        assertThat(value.wasTruncated()).isEqualTo(expected.wasTruncated());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("ru.aasmc.tracing.collector.testmodel.domain.AttributeMatrix#rejected")
    @DisplayName("rejects null values and doubles that are not finite")
    void given_invalidValue_when_constructed_then_rejected(AttributeCase attributeCase) {
        Expectation.Rejected expected = (Expectation.Rejected) attributeCase.expected();

        assertViolation(expected.code(), () -> attributeCase.construct().get());
    }

    @Test
    @DisplayName("an array value is an immutable copy of the given list")
    void given_mutableList_when_arrayConstructed_then_immutableCopy() {
        List<Long> source = new ArrayList<>(List.of(1L));

        LongArrayValue value = new LongArrayValue(source);
        source.add(2L);

        assertThat(value.values()).containsExactly(1L);
        assertThatThrownBy(() -> value.values().add(3L)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("the cutting factories and the array constructors reject null input with INVALID_ATTRIBUTE_TYPE")
    void given_nullInput_when_factoriesOrArraysUsed_then_invalidAttributeType() {
        assertViolation(ViolationCode.INVALID_ATTRIBUTE_TYPE, () -> AttributeValue.StringValue.of(null, 10));
        assertViolation(
                ViolationCode.INVALID_ATTRIBUTE_TYPE,
                () -> AttributeValue.StringArrayValue.of(Arrays.asList("a", null), 10));
        assertViolation(ViolationCode.INVALID_ATTRIBUTE_TYPE, () -> new AttributeValue.BoolArrayValue(null));
        assertViolation(
                ViolationCode.INVALID_ATTRIBUTE_TYPE, () -> new AttributeValue.LongArrayValue(Arrays.asList(1L, null)));
    }
}
