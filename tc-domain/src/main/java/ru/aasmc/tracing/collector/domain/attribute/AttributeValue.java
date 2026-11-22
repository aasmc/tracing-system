package ru.aasmc.tracing.collector.domain.attribute;

import java.util.List;
import java.util.OptionalDouble;

/**
 * Typed OTel attribute value: exactly the eight types of {@link AttributeType} (05 §3.4).
 *
 * <p>Common invariants ({@code INVALID_ATTRIBUTE_TYPE}): no null values or null array elements; string values are at
 * most {@link #MAX_STRING_BYTES} UTF-8 bytes (use {@link StringValue#of} to cut and flag); doubles are never NaN
 * (NaN cannot be indexed or stored in jsonb), infinities are rejected too (jsonb has no representation); arrays are
 * immutable copies. LONG values are kept as {@code long}, never through {@code double}, so values above 2^53 survive.
 */
public sealed interface AttributeValue {

    /** 4 KiB, Tempo-like. */
    int MAX_STRING_BYTES = 4 * 1024;

    AttributeType type();

    /**
     * Canonical tokens for {@code trace_attribute.value_str}, one per array element: strings as is, booleans as
     * {@code true}/{@code false}, LONG as decimal, DOUBLE via {@link Double#toString(double)}. Not yet cut to the
     * 512-byte index limit ({@code IngestLimits.truncateIndexValue}).
     */
    List<String> indexTokens();

    /** Value for {@code trace_attribute.value_num}: LONG and DOUBLE scalars only. */
    default OptionalDouble numeric() {
        throw new UnsupportedOperationException();
    }

    /** {@code true} if a string (or string element) was cut to {@link #MAX_STRING_BYTES}. */
    default boolean truncated() {
        throw new UnsupportedOperationException();
    }

    /**
     * STRING.
     *
     * @param value the text, at most 4 KiB of UTF-8
     * @param truncated whether {@link #of(String)} cut it
     */
    record StringValue(String value, boolean truncated) implements AttributeValue {

        /** Cuts to 4 KiB of UTF-8 without splitting a surrogate pair and sets {@code truncated}. */
        public static StringValue of(String raw) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexTokens() {
            throw new UnsupportedOperationException();
        }
    }

    /** BOOLEAN. */
    record BoolValue(boolean value) implements AttributeValue {

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexTokens() {
            throw new UnsupportedOperationException();
        }
    }

    /** LONG, full 64-bit precision. */
    record LongValue(long value) implements AttributeValue {

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexTokens() {
            throw new UnsupportedOperationException();
        }
    }

    /** DOUBLE, finite. Negative zero is kept as is. */
    record DoubleValue(double value) implements AttributeValue {

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexTokens() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * STRING_ARRAY.
     *
     * @param values elements, each at most 4 KiB of UTF-8
     * @param truncated whether {@link #of(List)} cut an element
     */
    record StringArrayValue(List<String> values, boolean truncated) implements AttributeValue {

        /** Cuts every element like {@link StringValue#of(String)}. */
        public static StringArrayValue of(List<String> raw) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexTokens() {
            throw new UnsupportedOperationException();
        }
    }

    /** BOOLEAN_ARRAY. */
    record BoolArrayValue(List<Boolean> values) implements AttributeValue {

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexTokens() {
            throw new UnsupportedOperationException();
        }
    }

    /** LONG_ARRAY. */
    record LongArrayValue(List<Long> values) implements AttributeValue {

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexTokens() {
            throw new UnsupportedOperationException();
        }
    }

    /** DOUBLE_ARRAY, finite elements. */
    record DoubleArrayValue(List<Double> values) implements AttributeValue {

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexTokens() {
            throw new UnsupportedOperationException();
        }
    }
}
