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
        throw new UnsupportedOperationException();
    }
}
