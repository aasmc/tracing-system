package ru.aasmc.tracing.collector.domain.attribute;

import java.util.Map;

/**
 * Decides which attribute keys hold personal data or secrets, and how their values are shown.
 *
 * <p>A rule's key is either an exact attribute key ({@code user_id}) or a pattern with {@code *} matching any
 * characters ({@code *.phone}). An exact rule wins over a pattern; keys without a rule are {@link Sensitivity#NONE}.
 *
 * @param rules exact keys or patterns, and their class
 */
public record SensitivityPolicy(Map<String, Sensitivity> rules) {

    public Sensitivity sensitivityOf(AttributeKey key) {
        throw new UnsupportedOperationException();
    }

    /**
     * The value as a caller may see it: SECRET values are always masked; PERSONAL values are masked unless the caller
     * may see personal data; other values are shown as they are. Masking works like {@link DigestTransform#MASK}.
     */
    public String displayValue(AttributeKey key, String value, boolean callerMaySeePersonalData) {
        throw new UnsupportedOperationException();
    }
}
