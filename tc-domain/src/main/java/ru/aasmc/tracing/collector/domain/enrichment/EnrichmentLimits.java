package ru.aasmc.tracing.collector.domain.enrichment;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import ru.aasmc.tracing.collector.domain.Limits;
import ru.aasmc.tracing.collector.domain.Names;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * Configurable limits of enrichment sources. All limits are positive; {@link #DEFAULT} holds the values used when none
 * are configured.
 *
 * @param maxNameLength characters of a source name
 * @param maxFieldMappings mappings of one source
 * @param maxTargetKeyLength characters of a mapping's target key
 * @param maxMaskKeepLastChars characters a {@link Transform.Mask} may leave visible
 * @param maxRegexLength characters of a {@link Transform.RegexExtract} expression
 */
public record EnrichmentLimits(
        int maxNameLength, int maxFieldMappings, int maxTargetKeyLength, int maxMaskKeepLastChars, int maxRegexLength) {

    public static final EnrichmentLimits DEFAULT = new EnrichmentLimits(63, 50, 128, 16, 256);

    public EnrichmentLimits {
        Limits.requirePositive("maxNameLength", maxNameLength);
        Limits.requirePositive("maxFieldMappings", maxFieldMappings);
        Limits.requirePositive("maxTargetKeyLength", maxTargetKeyLength);
        Limits.requirePositive("maxMaskKeepLastChars", maxMaskKeepLastChars);
        Limits.requirePositive("maxRegexLength", maxRegexLength);
    }

    /**
     * Checks that the source's name is not longer than allowed.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_NAME} for a too long name
     */
    public void requireWithin(EnrichmentSource source) {
        Names.requireAtMost(source.name(), maxNameLength);
    }

    /**
     * Checks that the definition stays within these limits: number of mappings, target-key length and transform
     * arguments.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_MAPPING} for too many mappings,
     *     {@code INVALID_TARGET_KEY} for a too long target key, {@code INVALID_TRANSFORM} for a transform argument above
     *     its limit
     */
    public void requireWithin(EnrichmentSourceDefinition definition) {
        require(
                definition.fieldMappings().size() <= maxFieldMappings,
                ViolationCode.INVALID_MAPPING,
                definition.fieldMappings().size() + " mappings, at most " + maxFieldMappings + " allowed");
        for (FieldMapping mapping : definition.fieldMappings()) {
            require(
                    mapping.targetKey().value().length() <= maxTargetKeyLength,
                    ViolationCode.INVALID_TARGET_KEY,
                    "target key longer than " + maxTargetKeyLength + " characters: "
                            + mapping.targetKey().value());
            requireWithin(mapping.transform());
        }
    }

    private void requireWithin(Transform transform) {
        switch (transform) {
            case Transform.Mask(int keepLastChars) ->
                require(
                        keepLastChars <= maxMaskKeepLastChars,
                        ViolationCode.INVALID_TRANSFORM,
                        "mask keeps at most " + maxMaskKeepLastChars + " characters, not " + keepLastChars);
            case Transform.RegexExtract(String regex) ->
                require(
                        regex.length() <= maxRegexLength,
                        ViolationCode.INVALID_TRANSFORM,
                        "regex longer than " + maxRegexLength + " characters");
            default -> {}
        }
    }
}
