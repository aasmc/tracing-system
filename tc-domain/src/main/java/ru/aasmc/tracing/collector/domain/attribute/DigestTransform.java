package ru.aasmc.tracing.collector.domain.attribute;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

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
    /**
     * Keep the first two characters (code points, as PostgreSQL {@code left(value, 2)} counts them) and append
     * {@code ***}, e.g. {@code u-42} → {@code u-***}.
     */
    MASK;

    private static final int MASK_KEPT_CODE_POINTS = 2;
    private static final String MASK_SUFFIX = "***";

    public String apply(String value) {
        return switch (this) {
            case NONE -> value;
            case SHA256 -> sha256Hex(value);
            case MASK -> mask(value);
        };
    }

    private static String sha256Hex(String value) {
        try {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(sha256.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("every Java runtime provides SHA-256", e);
        }
    }

    private static String mask(String value) {
        int keptCodePoints = Math.min(MASK_KEPT_CODE_POINTS, value.codePointCount(0, value.length()));
        return value.substring(0, value.offsetByCodePoints(0, keptCodePoints)) + MASK_SUFFIX;
    }
}
