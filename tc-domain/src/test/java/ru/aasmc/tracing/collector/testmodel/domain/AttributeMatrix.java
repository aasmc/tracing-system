package ru.aasmc.tracing.collector.testmodel.domain;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalDouble;
import java.util.function.Supplier;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.AttributeType;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.BoolArrayValue;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.BoolValue;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.DoubleArrayValue;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.DoubleValue;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.LongArrayValue;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.LongValue;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.StringArrayValue;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.StringValue;

/**
 * All eight attribute types with their edge cases. Strings are created with the cutting factories
 * {@code StringValue.of} and {@code StringArrayValue.of}, other values with their constructor; each case expects
 * either a valid value with the given properties or a rejection.
 */
public final class AttributeMatrix {

    public static final int LIMIT = AttributeValue.MAX_STRING_BYTES;

    /** 2^53 + 1: the smallest positive long that a {@code double} cannot represent. */
    public static final long ABOVE_DOUBLE_PRECISION = 9_007_199_254_740_993L;

    private AttributeMatrix() {}

    /** One matrix row. */
    public record AttributeCase(String name, Supplier<AttributeValue> construct, Expectation expected) {

        @Override
        public String toString() {
            return name;
        }
    }

    /** Expected outcome of constructing the value. */
    public sealed interface Expectation {

        /** The value is accepted and behaves like this. */
        record Valid(
                AttributeValue value,
                AttributeType type,
                List<String> indexValues,
                OptionalDouble numericValue,
                boolean wasTruncated)
                implements Expectation {}

        /** Construction throws {@code DomainViolationException} with this code. */
        record Rejected(ViolationCode code) implements Expectation {}
    }

    public static List<AttributeCase> all() {
        String fourKiB = "x".repeat(LIMIT);
        String fourKiBPlusOne = fourKiB + "y";
        // 2047 × 'é' (2 bytes) = 4094 bytes, then a 4-byte emoji: the cut must not split the emoji
        String multibyteOverflow = "é".repeat(2047) + "🌍";
        String multibyteCut = "é".repeat(2047);
        String unicode = "Привет, 世界 🌍";

        return List.of(
                string("string ascii", "hello", "hello", false),
                string("string empty", "", "", false),
                string("string unicode (Cyrillic, CJK, astral)", unicode, unicode, false),
                string("string with \\u0000", "a\u0000b", "a\u0000b", false),
                string("string exactly 4 KiB", fourKiB, fourKiB, false),
                string("string 4 KiB + 1 byte", fourKiBPlusOne, fourKiB, true),
                string("string 4 KiB + 2 bytes ending in a 4-byte code point", multibyteOverflow, multibyteCut, true),
                scalar("boolean true", new BoolValue(true), "true", OptionalDouble.empty()),
                scalar("boolean false", new BoolValue(false), "false", OptionalDouble.empty()),
                scalar("long 42", new LongValue(42), "42", OptionalDouble.of(42)),
                scalar(
                        "long 2^53 + 1 keeps precision",
                        new LongValue(ABOVE_DOUBLE_PRECISION),
                        "9007199254740993",
                        OptionalDouble.of((double) ABOVE_DOUBLE_PRECISION)),
                scalar(
                        "long min value",
                        new LongValue(Long.MIN_VALUE),
                        "-9223372036854775808",
                        OptionalDouble.of((double) Long.MIN_VALUE)),
                scalar("double 1.5", new DoubleValue(1.5), "1.5", OptionalDouble.of(1.5)),
                scalar("double negative zero", new DoubleValue(-0.0), "-0.0", OptionalDouble.of(-0.0)),
                rejected("double NaN", () -> new DoubleValue(Double.NaN)),
                rejected("double +Infinity", () -> new DoubleValue(Double.POSITIVE_INFINITY)),
                rejected("double -Infinity", () -> new DoubleValue(Double.NEGATIVE_INFINITY)),
                array(
                        "string array",
                        () -> StringArrayValue.of(List.of("a", "b")),
                        new StringArrayValue(List.of("a", "b"), false),
                        List.of("a", "b"),
                        false),
                array(
                        "string array with a 4 KiB + 1 element",
                        () -> StringArrayValue.of(List.of("ok", fourKiBPlusOne)),
                        new StringArrayValue(List.of("ok", fourKiB), true),
                        List.of("ok", fourKiB),
                        true),
                array(
                        "string array empty",
                        () -> StringArrayValue.of(List.of()),
                        new StringArrayValue(List.of(), false),
                        List.of(),
                        false),
                array(
                        "boolean array",
                        () -> new BoolArrayValue(List.of(true, false)),
                        new BoolArrayValue(List.of(true, false)),
                        List.of("true", "false"),
                        false),
                array(
                        "boolean array empty",
                        () -> new BoolArrayValue(List.of()),
                        new BoolArrayValue(List.of()),
                        List.of(),
                        false),
                array(
                        "long array with 2^53 + 1",
                        () -> new LongArrayValue(List.of(1L, ABOVE_DOUBLE_PRECISION)),
                        new LongArrayValue(List.of(1L, ABOVE_DOUBLE_PRECISION)),
                        List.of("1", "9007199254740993"),
                        false),
                array(
                        "long array empty",
                        () -> new LongArrayValue(List.of()),
                        new LongArrayValue(List.of()),
                        List.of(),
                        false),
                array(
                        "double array with negative zero",
                        () -> new DoubleArrayValue(List.of(0.5d, -0.0d)),
                        new DoubleArrayValue(List.of(0.5d, -0.0d)),
                        List.of("0.5", "-0.0"),
                        false),
                array(
                        "double array empty",
                        () -> new DoubleArrayValue(List.of()),
                        new DoubleArrayValue(List.of()),
                        List.of(),
                        false),
                rejected("double array with NaN", () -> new DoubleArrayValue(List.of(1.0d, Double.NaN))),
                rejected(
                        "string array with a null element",
                        () -> new StringArrayValue(Arrays.asList("a", null), false)),
                rejected("string null", () -> new StringValue(null, false)));
    }

    /** Only the rows whose value is valid. */
    public static List<AttributeCase> valid() {
        return all().stream()
                .filter(c -> c.expected() instanceof Expectation.Valid)
                .toList();
    }

    /** Only the rows whose construction is rejected. */
    public static List<AttributeCase> rejected() {
        return all().stream()
                .filter(c -> c.expected() instanceof Expectation.Rejected)
                .toList();
    }

    private static AttributeCase string(String name, String raw, String stored, boolean wasTruncated) {
        return new AttributeCase(
                name,
                () -> StringValue.of(raw),
                new Expectation.Valid(
                        new StringValue(stored, wasTruncated),
                        AttributeType.STRING,
                        List.of(stored),
                        OptionalDouble.empty(),
                        wasTruncated));
    }

    private static AttributeCase scalar(String name, AttributeValue value, String token, OptionalDouble numericValue) {
        return new AttributeCase(
                name, () -> value, new Expectation.Valid(value, typeOf(value), List.of(token), numericValue, false));
    }

    private static AttributeCase array(
            String name,
            Supplier<AttributeValue> construct,
            AttributeValue value,
            List<String> tokens,
            boolean wasTruncated) {
        return new AttributeCase(
                name,
                construct,
                new Expectation.Valid(value, typeOf(value), tokens, OptionalDouble.empty(), wasTruncated));
    }

    private static AttributeCase rejected(String name, Supplier<AttributeValue> construct) {
        return new AttributeCase(name, construct, new Expectation.Rejected(ViolationCode.INVALID_ATTRIBUTE_TYPE));
    }

    /** The type a value must report, derived from its class. */
    private static AttributeType typeOf(AttributeValue value) {
        return switch (value) {
            case StringValue _ -> AttributeType.STRING;
            case BoolValue _ -> AttributeType.BOOLEAN;
            case LongValue _ -> AttributeType.LONG;
            case DoubleValue _ -> AttributeType.DOUBLE;
            case StringArrayValue _ -> AttributeType.STRING_ARRAY;
            case BoolArrayValue _ -> AttributeType.BOOLEAN_ARRAY;
            case LongArrayValue _ -> AttributeType.LONG_ARRAY;
            case DoubleArrayValue _ -> AttributeType.DOUBLE_ARRAY;
        };
    }
}
