package ru.aasmc.tracing.collector.domain.attribute;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SequencedMap;

/**
 * Ordered, immutable attribute map; the producer's order is kept for display (05 §3.4).
 *
 * <p>Invariants: at most {@link #MAX_ENTRIES} entries ({@code TOO_MANY_ATTRIBUTES}); no null keys or values;
 * {@code entries} is an unmodifiable copy.
 *
 * @param entries the attributes in producer order
 */
public record Attributes(SequencedMap<AttributeKey, AttributeValue> entries) {

    public static final int MAX_ENTRIES = 512;

    /** No attributes. */
    public static Attributes empty() {
        throw new UnsupportedOperationException();
    }

    /** Copies {@code entries}, keeping their iteration order. */
    public static Attributes of(Map<AttributeKey, AttributeValue> entries) {
        throw new UnsupportedOperationException();
    }

    public Optional<AttributeValue> get(AttributeKey key) {
        throw new UnsupportedOperationException();
    }

    public int size() {
        throw new UnsupportedOperationException();
    }

    public boolean isEmpty() {
        throw new UnsupportedOperationException();
    }

    /** {@code true} if any value was cut to 4 KiB. */
    public boolean anyTruncated() {
        throw new UnsupportedOperationException();
    }

    /** Entries sorted by key: the canonical order used by the content hash (05 §3.5). */
    public List<Map.Entry<AttributeKey, AttributeValue>> sortedByKey() {
        throw new UnsupportedOperationException();
    }
}
