package ru.aasmc.tracing.collector.domain.enrichment;

import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeType;

/**
 * Reads one value from an enrichment event, transforms it and stores it as one attribute.
 *
 * <p>Rules:
 *
 * <ul>
 *   <li>PAYLOAD needs {@code jsonPointer}; HEADER needs a non-blank {@code headerName} ({@code INVALID_MAPPING});
 *   <li>{@code targetKey} is 1..128 characters of {@code a-z 0-9 _ . -}, starting with a letter
 *       ({@code INVALID_TARGET_KEY}), and {@link AttributeKey#allowedAsEnrichmentTarget() allowed as an enrichment
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
        boolean required) {}
