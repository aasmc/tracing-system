package ru.aasmc.tracing.collector.domain.attribute;

/**
 * Presentation-time masking of attribute values (05 §7.3). Data stays raw in the database; masking applies to every
 * value the Query context returns: span, resource and enrichment attributes, matched search attributes, correlation
 * values and display values.
 *
 * <p>Rules: NONE is shown as is; PERSONAL is shown only to callers with scope {@code TC_PII}, masked otherwise;
 * SECRET is always masked. A digest value hashed with SHA256 is still masked by its key's class, so a hashed
 * PERSONAL value is shown only with {@code TC_PII}. Stateless apart from the immutable policy; thread-safe.
 *
 * @param policy the key classification
 */
public record PiiMasker(PiiPolicy policy) {

    /** The value as the caller may see it. */
    public String render(AttributeKey key, String value, boolean callerHasPiiScope) {
        throw new UnsupportedOperationException();
    }

    /**
     * 05 §7.3: PREFIX search and value autocomplete on a PERSONAL key need {@code TC_PII}; on a SECRET key they are
     * never allowed. EQ is always allowed (the caller already knows the value).
     */
    public boolean mayEnumerate(AttributeKey key, boolean callerHasPiiScope) {
        throw new UnsupportedOperationException();
    }

    /**
     * The mask: {@code **} for values of at most 2 characters, otherwise the first {@code min(2, length / 3)}
     * characters followed by {@code **} (e.g. {@code u-42} → {@code u**}).
     */
    public static String mask(String value) {
        throw new UnsupportedOperationException();
    }
}
