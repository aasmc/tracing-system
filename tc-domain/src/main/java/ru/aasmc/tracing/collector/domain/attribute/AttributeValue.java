package ru.aasmc.tracing.collector.domain.attribute;

import java.util.List;
import java.util.OptionalDouble;

/**
 * A typed attribute value: one of the eight OpenTelemetry types.
 *
 * <p>Rules for all types ({@code INVALID_ATTRIBUTE_TYPE}): no {@code null} values or array elements; strings are at
 * most {@value #MAX_STRING_BYTES} UTF-8 bytes (use {@link StringValue#of} to cut longer input); doubles are finite,
 * because PostgreSQL {@code jsonb} cannot store NaN or infinity; arrays are immutable copies. LONG values stay
 * {@code long} and never pass through {@code double}, so values above 2^53 keep every digit.
 */
public sealed interface AttributeValue {

    int MAX_STRING_BYTES = 4 * 1024;

    AttributeType type();

    /**
     * The values as they go into the search index, one per array element: strings as they are, booleans as
     * {@code true}/{@code false}, LONG in decimal, DOUBLE as {@link Double#toString(double)}. Not cut to the index
     * limit yet.
     */
    List<String> indexValues();

    /** The number for range search: present for LONG and DOUBLE only. */
    default OptionalDouble numericValue() {
        throw new UnsupportedOperationException();
    }

    /** Whether a string (or an array element) was cut to {@link #MAX_STRING_BYTES}. */
    default boolean wasTruncated() {
        throw new UnsupportedOperationException();
    }

    /**
     * @param value at most 4 KiB of UTF-8
     * @param wasTruncated whether {@link #of} cut it
     */
    record StringValue(String value, boolean wasTruncated) implements AttributeValue {

        /** Cuts {@code raw} to 4 KiB of UTF-8 without splitting a character and records whether it did. */
        public static StringValue of(String raw) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexValues() {
            throw new UnsupportedOperationException();
        }
    }

    record BoolValue(boolean value) implements AttributeValue {

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexValues() {
            throw new UnsupportedOperationException();
        }
    }

    record LongValue(long value) implements AttributeValue {

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexValues() {
            throw new UnsupportedOperationException();
        }
    }

    /** A finite double; negative zero is kept. */
    record DoubleValue(double value) implements AttributeValue {

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexValues() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * @param values elements, each at most 4 KiB of UTF-8
     * @param wasTruncated whether {@link #of} cut an element
     */
    record StringArrayValue(List<String> values, boolean wasTruncated) implements AttributeValue {

        /** Cuts every element like {@link StringValue#of}. */
        public static StringArrayValue of(List<String> raw) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexValues() {
            throw new UnsupportedOperationException();
        }
    }

    record BoolArrayValue(List<Boolean> values) implements AttributeValue {

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexValues() {
            throw new UnsupportedOperationException();
        }
    }

    record LongArrayValue(List<Long> values) implements AttributeValue {

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexValues() {
            throw new UnsupportedOperationException();
        }
    }

    /** Finite elements. */
    record DoubleArrayValue(List<Double> values) implements AttributeValue {

        @Override
        public AttributeType type() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> indexValues() {
            throw new UnsupportedOperationException();
        }
    }
}
