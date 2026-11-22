package ru.aasmc.tracing.collector.domain.enrichment;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import ru.aasmc.tracing.collector.domain.ViolationCode;
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

/**
 * Reads one value from an enrichment event, transforms it and stores it as one attribute.
 *
 * <p>Example: {@code PAYLOAD /status → sms.delivery.status (STRING)} reads the string {@code "DELIVERED"} from the
 * payload {@code {"status":"DELIVERED"}} and adds the attribute {@code sms.delivery.status = DELIVERED}.
 *
 * <p>Rules:
 *
 * <ul>
 *   <li>PAYLOAD needs {@code jsonPointer}; HEADER needs a non-blank {@code headerName}; {@code valueType} is STRING,
 *       BOOLEAN, LONG or DOUBLE, because an event field holds one value ({@code INVALID_MAPPING});
 *   <li>{@code targetKey} consists of {@code a-z 0-9 _ . -}, starting with a letter ({@code INVALID_TARGET_KEY}; the
 *       maximum length is configured in {@link EnrichmentLimits}), and {@link AttributeKey#allowedAsEnrichmentTarget() allowed as an enrichment
 *       target} ({@code RESERVED_KEY});
 *   <li>a transform other than {@link Transform.None} needs {@code valueType} STRING ({@code INVALID_TRANSFORM});
 *   <li>RECORD_TIMESTAMP maps to LONG (epoch milliseconds) or STRING (ISO-8601) ({@code INVALID_MAPPING});
 *   <li>a {@code null} transform becomes {@link Transform.None}.
 * </ul>
 *
 * @param source where the value is read
 * @param jsonPointer pointer for PAYLOAD, else {@code null}
 * @param headerName header for HEADER, else {@code null}
 * @param targetKey the attribute to write
 * @param valueType type of the written value
 * @param transform the transform
 * @param required whether an event without the value is rejected instead of skipping the attribute
 */
public record FieldMapping(
        FieldSource source,
        JsonPointer jsonPointer,
        String headerName,
        AttributeKey targetKey,
        AttributeType valueType,
        Transform transform,
        boolean required) {

    private static final Pattern TARGET_KEY_SYNTAX = Pattern.compile("[a-z][a-z0-9_.-]*");
    private static final Set<AttributeType> RECORD_TIMESTAMP_TYPES = Set.of(AttributeType.LONG, AttributeType.STRING);
    private static final Set<AttributeType> SCALAR_TYPES =
            Set.of(AttributeType.STRING, AttributeType.BOOLEAN, AttributeType.LONG, AttributeType.DOUBLE);

    public FieldMapping {
        require(source != null && valueType != null, ViolationCode.INVALID_MAPPING, "mapping without source or type");
        require(
                source != FieldSource.PAYLOAD || jsonPointer != null,
                ViolationCode.INVALID_MAPPING,
                "a PAYLOAD mapping needs a JSON Pointer");
        require(
                source != FieldSource.HEADER || (headerName != null && !headerName.isBlank()),
                ViolationCode.INVALID_MAPPING,
                "a HEADER mapping needs a header name");
        require(
                SCALAR_TYPES.contains(valueType),
                ViolationCode.INVALID_MAPPING,
                "a mapping writes one value, not an array: " + valueType);
        require(
                source != FieldSource.RECORD_TIMESTAMP || RECORD_TIMESTAMP_TYPES.contains(valueType),
                ViolationCode.INVALID_MAPPING,
                "the record timestamp maps to LONG or STRING, not " + valueType);
        require(
                targetKey != null
                        && TARGET_KEY_SYNTAX.matcher(targetKey.value()).matches(),
                ViolationCode.INVALID_TARGET_KEY,
                "invalid target key: " + targetKey);
        require(
                targetKey.allowedAsEnrichmentTarget(),
                ViolationCode.RESERVED_KEY,
                "enrichment cannot write the reserved key " + targetKey.value());
        transform = transform == null ? new Transform.None() : transform;
        require(
                transform instanceof Transform.None || valueType == AttributeType.STRING,
                ViolationCode.INVALID_TRANSFORM,
                "a transform works on strings only, not on " + valueType);
    }

    /**
     * Reads this mapping's value from {@code event} in three steps:
     *
     * <ol>
     *   <li>read the raw value from the place the mapping names; a value that is not there is missing;
     *   <li>turn it into an attribute value of {@code valueType}: a STRING is the text of the raw value, transformed
     *       ({@link ExtractionFailure#TRANSFORM_FAILED} if the transform gives nothing) and cut to
     *       {@code maxValueBytes}; BOOLEAN, LONG and DOUBLE accept a JSON value or text of that type
     *       ({@link ExtractionFailure#TYPE_MISMATCH} otherwise, e.g. {@code 4.5} or {@code "abc"} for LONG);
     *   <li>a missing value is {@link ExtractionFailure#MISSING_REQUIRED_FIELD} if the mapping is required.
     * </ol>
     */
    FieldReading<AttributeValue> read(EnrichmentEvent event, int maxValueBytes) {
        FieldReading<JsonScalar> raw = rawValue(event).map(FieldReading::found).orElseGet(FieldReading::missing);
        return raw.then(value ->
                        valueType == AttributeType.STRING ? transformedString(value, maxValueBytes) : typed(value))
                .requiredIf(required);
    }

    /**
     * The value at the place this mapping reads from, or empty if the event has none there. Headers and the record key
     * are read as text; the record timestamp as epoch milliseconds (LONG mapping) or ISO-8601 text (STRING mapping).
     */
    private Optional<JsonScalar> rawValue(EnrichmentEvent event) {
        return switch (source) {
            case PAYLOAD -> event.payloadValue(jsonPointer);
            case HEADER -> event.header(headerName).map(StringScalar::new);
            case RECORD_KEY -> event.recordKey().map(StringScalar::new);
            case RECORD_TIMESTAMP -> Optional.of(timestampOf(event.recordTimestamp()));
        };
    }

    private JsonScalar timestampOf(Instant timestamp) {
        return valueType == AttributeType.LONG
                ? new NumberScalar(BigDecimal.valueOf(timestamp.toEpochMilli()))
                : new StringScalar(timestamp.toString());
    }

    /** A STRING value: the text of the raw value, transformed and cut to {@code maxValueBytes}. */
    private FieldReading<AttributeValue> transformedString(JsonScalar raw, int maxValueBytes) {
        return transform
                .apply(textOf(raw))
                .<AttributeValue>map(text -> StringValue.of(text, maxValueBytes))
                .map(FieldReading::found)
                .orElseGet(() -> FieldReading.failed(ExtractionFailure.TRANSFORM_FAILED));
    }

    /** A BOOLEAN, LONG or DOUBLE value converted from the raw value, or {@code TYPE_MISMATCH} if it does not convert. */
    private FieldReading<AttributeValue> typed(JsonScalar raw) {
        Optional<AttributeValue> value = switch (valueType) {
            case BOOLEAN -> booleanOf(raw).map(BoolValue::new);
            case LONG -> numberOf(raw).flatMap(FieldMapping::exactLong).map(LongValue::new);
            case DOUBLE ->
                numberOf(raw)
                        .map(BigDecimal::doubleValue)
                        .filter(Double::isFinite)
                        .map(DoubleValue::new);
            default -> throw new IllegalStateException("not a scalar type: " + valueType);
        };
        return value.map(FieldReading::found).orElseGet(() -> FieldReading.failed(ExtractionFailure.TYPE_MISMATCH));
    }

    private static String textOf(JsonScalar raw) {
        return switch (raw) {
            case StringScalar(String text) -> text;
            case NumberScalar(BigDecimal number) -> number.toString(); // 1e3 gives 1E+3: never expands a huge exponent
            case BooleanScalar(boolean flag) -> Boolean.toString(flag);
        };
    }

    /** A JSON boolean, or the text {@code true} or {@code false}. */
    private static Optional<Boolean> booleanOf(JsonScalar raw) {
        return switch (raw) {
            case BooleanScalar(boolean flag) -> Optional.of(flag);
            case StringScalar(String text)
            when text.equals("true") || text.equals("false") -> Optional.of(Boolean.parseBoolean(text));
            default -> Optional.empty();
        };
    }

    /** A JSON number, or text holding a decimal number such as {@code "42"} or {@code "4.5"}. */
    private static Optional<BigDecimal> numberOf(JsonScalar raw) {
        return switch (raw) {
            case NumberScalar(BigDecimal number) -> Optional.of(number);
            case StringScalar(String text) -> parseDecimal(text);
            case BooleanScalar _ -> Optional.empty();
        };
    }

    private static Optional<BigDecimal> parseDecimal(String text) {
        try {
            return Optional.of(new BigDecimal(text));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    /** The number as a {@code long} if it has no fraction and fits; {@code 42.0} counts as 42. */
    private static Optional<Long> exactLong(BigDecimal number) {
        try {
            return Optional.of(number.longValueExact());
        } catch (ArithmeticException e) {
            return Optional.empty();
        }
    }
}
