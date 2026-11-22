package ru.aasmc.tracing.collector.domain.attribute;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import ru.aasmc.tracing.collector.domain.Limits;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * Configurable size limits of attributes. All limits are positive.
 *
 * @param maxKeyLength characters of a key
 * @param maxValueBytes UTF-8 bytes of a string value or array element; longer input is cut by
 *     {@link AttributeValue.StringValue#of} and {@link AttributeValue.StringArrayValue#of}
 * @param maxEntries attributes of one span, event, link, resource or scope
 */
public record AttributeLimits(int maxKeyLength, int maxValueBytes, int maxEntries) {

    public static final AttributeLimits DEFAULT = new AttributeLimits(255, 4 * 1024, 512);

    public AttributeLimits {
        Limits.requirePositive("maxKeyLength", maxKeyLength);
        Limits.requirePositive("maxValueBytes", maxValueBytes);
        Limits.requirePositive("maxEntries", maxEntries);
    }

    /**
     * Checks that {@code attributes} stay within these limits.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code TOO_MANY_ATTRIBUTES} for more than
     *     {@code maxEntries} attributes, {@code INVALID_ATTRIBUTE_KEY} for a key longer than {@code maxKeyLength}
     */
    public void requireWithin(Attributes attributes) {
        require(
                attributes.entries().size() <= maxEntries,
                ViolationCode.TOO_MANY_ATTRIBUTES,
                attributes.entries().size() + " attributes, at most " + maxEntries + " allowed");
        for (AttributeKey key : attributes.entries().keySet()) {
            require(
                    key.value().length() <= maxKeyLength,
                    ViolationCode.INVALID_ATTRIBUTE_KEY,
                    "attribute key longer than " + maxKeyLength + " characters: " + key.value());
        }
    }
}
