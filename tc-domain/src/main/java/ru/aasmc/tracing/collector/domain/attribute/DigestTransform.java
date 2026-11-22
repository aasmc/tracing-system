package ru.aasmc.tracing.collector.domain.attribute;

/**
 * How a retained value is written into {@code trace_digest.attributes} (05 §4.9, ADR-13); stored in
 * {@code indexed_attribute.digest_transform}. {@code tc-query-api} applies the same function to search values of
 * digest-tier queries.
 */
public enum DigestTransform {
    /** As is. */
    NONE,
    /** {@link DigestValueHasher#sha256Hex(String)}: joinable and searchable by equality, not readable. */
    SHA256,
    /** {@link DigestValueHasher#mask(String)}: recognisable prefix only, not searchable. */
    MASK;

    public String apply(String value) {
        throw new UnsupportedOperationException();
    }

    /** EQ/IN in the digest tier work for NONE and SHA256 (hash the query value); MASK destroys the value. */
    public boolean searchableByEquality() {
        throw new UnsupportedOperationException();
    }
}
