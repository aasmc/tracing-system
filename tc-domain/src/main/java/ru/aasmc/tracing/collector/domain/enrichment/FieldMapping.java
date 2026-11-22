package ru.aasmc.tracing.collector.domain.enrichment;

import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeType;

/**
 * Takes one field of an enrichment event, applies a {@link Transform}, and produces one attribute (05 §4.7.1).
 *
 * <p>Invariants: PAYLOAD needs {@code jsonPointer}, HEADER needs a non-blank {@code headerName}
 * ({@code INVALID_MAPPING}); {@code targetKey} matches {@link #TARGET_KEY_PATTERN} ({@code INVALID_TARGET_KEY}) and
 * is not {@link AttributeKey#reservedForEnrichment() reserved} ({@code RESERVED_KEY}); a string-only transform needs
 * {@code valueType} STRING, and RECORD_TIMESTAMP maps to LONG (epoch ms) or STRING (ISO-8601)
 * ({@code INVALID_TRANSFORM}, {@code INVALID_MAPPING}); a {@code null} transform becomes {@link Transform.None}.
 *
 * @param source where the value is read
 * @param jsonPointer pointer for PAYLOAD, else {@code null}
 * @param headerName header for HEADER, else {@code null}
 * @param targetKey produced attribute key
 * @param valueType declared type of the produced value
 * @param transform value transform
 * @param required a missing value sends the event to the DLQ ({@code REQUIRED_FIELD_MISSING})
 */
public record FieldMapping(
        FieldSource source,
        JsonPointerExpr jsonPointer,
        String headerName,
        AttributeKey targetKey,
        AttributeType valueType,
        Transform transform,
        boolean required) {

    /** 03 §5.4 V6. */
    public static final String TARGET_KEY_PATTERN = "^[a-z][a-z0-9_.-]{0,127}$";
}
