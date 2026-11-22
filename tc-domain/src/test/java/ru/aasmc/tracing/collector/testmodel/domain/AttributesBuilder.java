package ru.aasmc.tracing.collector.testmodel.domain;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.SequencedMap;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue;
import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/** Builds {@link Attributes} in insertion order. */
public final class AttributesBuilder {

    private final SequencedMap<AttributeKey, AttributeValue> entries = new LinkedHashMap<>();

    public static AttributesBuilder attributes() {
        return new AttributesBuilder();
    }

    public static Attributes empty() {
        return attributes().build();
    }

    public AttributesBuilder put(String key, String value) {
        return put(key, new AttributeValue.StringValue(value, false));
    }

    public AttributesBuilder put(String key, long value) {
        return put(key, new AttributeValue.LongValue(value));
    }

    public AttributesBuilder put(String key, AttributeValue value) {
        entries.put(new AttributeKey(key), value);
        return this;
    }

    public Attributes build() {
        return new Attributes(Collections.unmodifiableSequencedMap(new LinkedHashMap<>(entries)));
    }
}
