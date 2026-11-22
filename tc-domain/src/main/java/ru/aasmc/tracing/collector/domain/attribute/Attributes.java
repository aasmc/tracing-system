package ru.aasmc.tracing.collector.domain.attribute;

import java.util.SequencedMap;

/**
 * An immutable attribute map in the producer's order, which is kept for display.
 *
 * <p>At most {@value #MAX_ENTRIES} entries ({@code TOO_MANY_ATTRIBUTES}); no {@code null} keys or values.
 *
 * @param entries the attributes
 */
public record Attributes(SequencedMap<AttributeKey, AttributeValue> entries) {

    public static final int MAX_ENTRIES = 512;

    public static Attributes empty() {
        throw new UnsupportedOperationException();
    }

    /** Whether any value was cut to the string size limit. */
    public boolean anyValueTruncated() {
        throw new UnsupportedOperationException();
    }
}
