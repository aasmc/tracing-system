package ru.aasmc.tracing.collector.domain.attribute;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.util.List;
import java.util.OptionalDouble;
import java.util.function.Predicate;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * A typed attribute value: one of the eight OpenTelemetry types.
 *
 * <p>Rules for all types ({@code INVALID_ATTRIBUTE_TYPE}): no {@code null} values or array elements; doubles are
 * finite, because PostgreSQL {@code jsonb} cannot store NaN or infinity; arrays are immutable copies. Strings are cut
 * to the configured {@link AttributeLimits#maxValueBytes()} by {@link StringValue#of} and {@link StringArrayValue#of}. LONG values stay
 * {@code long} and never pass through {@code double}, so values above 2^53 keep every digit.
 */
public sealed interface AttributeValue {

    AttributeType type();

    /**
     * The values as they go into the search index, one per array element: strings as they are, booleans as
     * {@code true}/{@code false}, LONG in decimal, DOUBLE as {@link Double#toString(double)}. Not cut to the index
     * limit yet.
     */
    List<String> indexValues();

    /** The number for range search: present for LONG and DOUBLE only. */
    default OptionalDouble numericValue() {
        return switch (this) {
            case LongValue(long value) -> OptionalDouble.of(value);
            case DoubleValue(double value) -> OptionalDouble.of(value);
            default -> OptionalDouble.empty();
        };
    }

    /** Whether a string (or an array element) was cut to the size limit. */
    default boolean wasTruncated() {
        return false;
    }

    /**
     * @param value the string
     * @param wasTruncated whether {@link #of} cut it
     */
    record StringValue(String value, boolean wasTruncated) implements AttributeValue {

        public StringValue {
            require(value != null, ViolationCode.INVALID_ATTRIBUTE_TYPE, "string is null");
        }

        /** Cuts {@code raw} to {@code maxBytes} of UTF-8 without splitting a character and records whether it did. */
        public static StringValue of(String raw, int maxBytes) {
            require(raw != null, ViolationCode.INVALID_ATTRIBUTE_TYPE, "string is null");
            TruncatedText text = TruncatedText.truncate(raw, maxBytes);
            return new StringValue(text.value(), text.wasTruncated());
        }

        @Override
        public AttributeType type() {
            return AttributeType.STRING;
        }

        @Override
        public List<String> indexValues() {
            return List.of(value);
        }
    }

    record BoolValue(boolean value) implements AttributeValue {

        @Override
        public AttributeType type() {
            return AttributeType.BOOLEAN;
        }

        @Override
        public List<String> indexValues() {
            return List.of(Boolean.toString(value));
        }
    }

    record LongValue(long value) implements AttributeValue {

        @Override
        public AttributeType type() {
            return AttributeType.LONG;
        }

        @Override
        public List<String> indexValues() {
            return List.of(Long.toString(value));
        }
    }

    /** A finite double; negative zero is kept. */
    record DoubleValue(double value) implements AttributeValue {

        public DoubleValue {
            require(Double.isFinite(value), ViolationCode.INVALID_ATTRIBUTE_TYPE, "double is not finite: " + value);
        }

        @Override
        public AttributeType type() {
            return AttributeType.DOUBLE;
        }

        @Override
        public List<String> indexValues() {
            return List.of(Double.toString(value));
        }
    }

    /**
     * @param values elements
     * @param wasTruncated whether {@link #of} cut an element
     */
    record StringArrayValue(List<String> values, boolean wasTruncated) implements AttributeValue {

        public StringArrayValue {
            values = validCopy(values, _ -> true);
        }

        /** Cuts every element like {@link StringValue#of}. */
        public static StringArrayValue of(List<String> raw, int maxBytes) {
            List<StringValue> elements = raw.stream()
                    .map(element -> StringValue.of(element, maxBytes))
                    .toList();
            return new StringArrayValue(
                    elements.stream().map(StringValue::value).toList(),
                    elements.stream().anyMatch(StringValue::wasTruncated));
        }

        @Override
        public AttributeType type() {
            return AttributeType.STRING_ARRAY;
        }

        @Override
        public List<String> indexValues() {
            return values;
        }
    }

    record BoolArrayValue(List<Boolean> values) implements AttributeValue {

        public BoolArrayValue {
            values = validCopy(values, _ -> true);
        }

        @Override
        public AttributeType type() {
            return AttributeType.BOOLEAN_ARRAY;
        }

        @Override
        public List<String> indexValues() {
            return values.stream().map(String::valueOf).toList();
        }
    }

    record LongArrayValue(List<Long> values) implements AttributeValue {

        public LongArrayValue {
            values = validCopy(values, _ -> true);
        }

        @Override
        public AttributeType type() {
            return AttributeType.LONG_ARRAY;
        }

        @Override
        public List<String> indexValues() {
            return values.stream().map(String::valueOf).toList();
        }
    }

    /** Finite elements. */
    record DoubleArrayValue(List<Double> values) implements AttributeValue {

        public DoubleArrayValue {
            values = validCopy(values, Double::isFinite);
        }

        @Override
        public AttributeType type() {
            return AttributeType.DOUBLE_ARRAY;
        }

        @Override
        public List<String> indexValues() {
            return values.stream().map(String::valueOf).toList();
        }
    }

    /** An immutable copy of {@code values}, which must not be null and must hold only valid, non-null elements. */
    private static <T> List<T> validCopy(List<T> values, Predicate<T> isValid) {
        require(
                values != null && values.stream().allMatch(element -> element != null && isValid.test(element)),
                ViolationCode.INVALID_ATTRIBUTE_TYPE,
                "array is null or has a null or invalid element");
        return List.copyOf(values);
    }
}
