package ru.aasmc.tracing.collector.domain.trace;

/**
 * Strict parsing of the hex text of ids: only {@code 0-9}, {@code a-f} and {@code A-F}. {@link Long#parseUnsignedLong}
 * alone would also accept a leading {@code +}.
 */
final class Hex {

    private Hex() {}

    /** Whether {@code text[from, to)} consists of hex digits only. */
    static boolean isHex(String text, int from, int to) {
        for (int i = from; i < to; i++) {
            char c = text.charAt(i);
            boolean hexDigit = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
            if (!hexDigit) {
                return false;
            }
        }
        return true;
    }

    /** The 64 bits of the hex digits {@code text[from, to)}; call {@link #isHex} first. */
    static long toLong(String text, int from, int to) {
        return Long.parseUnsignedLong(text, from, to, 16);
    }
}
