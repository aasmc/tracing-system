package ru.aasmc.tracing.collector.domain.attribute;

/**
 * The Java side of the digest transforms (05 §4.9, ADR-13). It must produce exactly what {@code DigestCompactor}'s
 * SQL produces, which the data layer checks against PostgreSQL (09 P-23):
 *
 * <ul>
 *   <li>SHA256: {@code encode(sha256(convert_to(v, 'UTF8')), 'hex')}: 64 lowercase hex characters;
 *   <li>MASK: {@code left(v, 2) || '***'}: the first two code points (not UTF-16 units) followed by {@code ***}.
 * </ul>
 *
 * <p>Stateless and thread-safe.
 */
public final class DigestValueHasher {

    private DigestValueHasher() {}

    /** Lowercase hex SHA-256 of the UTF-8 bytes of {@code value}. */
    public static String sha256Hex(String value) {
        throw new UnsupportedOperationException();
    }

    /** First two code points of {@code value} (fewer if shorter) followed by {@code ***}. */
    public static String mask(String value) {
        throw new UnsupportedOperationException();
    }
}
