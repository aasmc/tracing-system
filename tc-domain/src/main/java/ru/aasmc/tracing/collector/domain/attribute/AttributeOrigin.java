package ru.aasmc.tracing.collector.domain.attribute;

/** Where an attribute value came from; stored as {@code origin smallint} (05 §1, §3.4). */
public enum AttributeOrigin {
    SPAN(1),
    RESOURCE(2),
    ENRICHMENT(3),
    /** Computed by TCS, e.g. {@code status=ERROR} or the root {@code span.name}. */
    DERIVED(4);

    private final short code;

    AttributeOrigin(int code) {
        this.code = (short) code;
    }

    public short code() {
        return code;
    }

    /**
     * Maps a stored code.
     *
     * @throws IllegalArgumentException for an unknown code
     */
    public static AttributeOrigin fromCode(short code) {
        throw new UnsupportedOperationException();
    }
}
