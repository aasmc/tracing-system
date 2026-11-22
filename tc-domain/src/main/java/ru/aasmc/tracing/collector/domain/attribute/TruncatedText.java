package ru.aasmc.tracing.collector.domain.attribute;

/**
 * A text cut to a byte limit, and whether it was cut.
 *
 * @param value the (possibly cut) text
 * @param wasTruncated whether anything was removed
 */
public record TruncatedText(String value, boolean wasTruncated) {

    /** Cuts {@code text} to at most {@code maxBytes} UTF-8 bytes without splitting a character. */
    public static TruncatedText truncate(String text, int maxBytes) {
        int bytes = 0;
        int end = 0;
        while (end < text.length()) {
            int codePoint = text.codePointAt(end);
            bytes += utf8Length(codePoint);
            if (bytes > maxBytes) {
                return new TruncatedText(text.substring(0, end), true);
            }
            end += Character.charCount(codePoint);
        }
        return new TruncatedText(text, false);
    }

    private static int utf8Length(int codePoint) {
        if (codePoint < 0x80) {
            return 1;
        }
        if (codePoint < 0x800) {
            return 2;
        }
        return codePoint < 0x10000 ? 3 : 4;
    }
}
