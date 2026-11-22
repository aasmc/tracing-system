package ru.aasmc.tracing.collector.domain.attribute;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.SequencedMap;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * An immutable attribute map in the producer's order, which is kept for display.
 *
 * <p>No {@code null} keys ({@code INVALID_ATTRIBUTE_KEY}) or values ({@code INVALID_ATTRIBUTE_TYPE}); a copy of the
 * given map. The number of entries is limited by {@link AttributeLimits}.
 *
 * @param entries the attributes
 */
public record Attributes(SequencedMap<AttributeKey, AttributeValue> entries) {

    private static final Attributes EMPTY = new Attributes(new LinkedHashMap<>());

    public Attributes {
        SequencedMap<AttributeKey, AttributeValue> copy = new LinkedHashMap<>(entries);
        require(!copy.containsKey(null), ViolationCode.INVALID_ATTRIBUTE_KEY, "attribute key is null");
        require(!copy.containsValue(null), ViolationCode.INVALID_ATTRIBUTE_TYPE, "attribute value is null");
        entries = Collections.unmodifiableSequencedMap(copy);
    }

    public static Attributes empty() {
        return EMPTY;
    }

    /** Whether any value was cut to the string size limit. */
    public boolean anyValueTruncated() {
        return entries.values().stream().anyMatch(AttributeValue::wasTruncated);
    }
}
