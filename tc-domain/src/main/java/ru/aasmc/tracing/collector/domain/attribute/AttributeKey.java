package ru.aasmc.tracing.collector.domain.attribute;

/**
 * OTel attribute key, interned through the {@code attribute_key} dictionary (05 §3.4).
 *
 * <p>Invariants ({@code INVALID_ATTRIBUTE_KEY}): not blank; at most {@link #MAX_LENGTH} characters; no control
 * characters. Any producer key is accepted, including {@code tc.*}: reserved names restrict only what TCS itself
 * writes (enrichment target keys, see {@link #reservedForEnrichment()}). Ordered by {@link String#compareTo}, the
 * order used by the content hash.
 *
 * @param value the key text
 */
public record AttributeKey(String value) implements Comparable<AttributeKey> {

    public static final int MAX_LENGTH = 255;

    /** Built-in indexed key, origin RESOURCE. */
    public static final AttributeKey SERVICE_NAME = new AttributeKey("service.name");

    /** Built-in indexed key, origin DERIVED, root spans only. */
    public static final AttributeKey SPAN_NAME = new AttributeKey("span.name");

    /** Built-in indexed key, origin DERIVED, value {@code ERROR} only. */
    public static final AttributeKey STATUS = new AttributeKey("status");

    /** {@code true} for the prefix {@code tc.}. */
    public boolean isReservedForTcs() {
        throw new UnsupportedOperationException();
    }

    /** {@code true} for {@link #SERVICE_NAME}, {@link #SPAN_NAME} and {@link #STATUS}. */
    public boolean isBuiltIn() {
        throw new UnsupportedOperationException();
    }

    /**
     * 03 §5.4 V7: keys an enrichment mapping may not target: prefixes {@code tc.}, {@code otel.}, {@code telemetry.},
     * {@code service.} and the built-ins.
     */
    public boolean reservedForEnrichment() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(AttributeKey other) {
        throw new UnsupportedOperationException();
    }
}
