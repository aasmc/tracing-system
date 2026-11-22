package ru.aasmc.tracing.collector.domain.attribute;

/**
 * How an attribute value is written into the long-term digest of a trace, which outlives the full trace data.
 * Personal data must not be kept there in clear text.
 */
public enum DigestTransform {
    /** Keep the value as it is. */
    NONE,
    /**
     * SHA-256 of the UTF-8 bytes as 64 lowercase hex characters. Equality search still works: the search value is
     * hashed the same way, in Java or in PostgreSQL.
     */
    SHA256,
    /** Keep the first two characters and append {@code ***}, e.g. {@code u-42} → {@code u-***}. */
    MASK;

    public String apply(String value) {
        throw new UnsupportedOperationException();
    }
}
