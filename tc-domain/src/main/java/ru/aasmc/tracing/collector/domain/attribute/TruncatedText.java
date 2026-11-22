package ru.aasmc.tracing.collector.domain.attribute;

/**
 * A string cut to a UTF-8 byte budget without splitting a code point (05 §3.4, 04 §5.5).
 *
 * <p>Invariants: {@code value} not null; if {@code truncated} is {@code false}, {@code value} is the input.
 *
 * @param value the (possibly cut) text
 * @param truncated whether anything was cut
 */
public record TruncatedText(String value, boolean truncated) {

    /**
     * Cuts {@code text} to at most {@code maxBytes} UTF-8 bytes; never splits a surrogate pair.
     *
     * @throws IllegalArgumentException if {@code maxBytes < 0} or {@code text} is null
     */
    public static TruncatedText truncate(String text, int maxBytes) {
        throw new UnsupportedOperationException();
    }
}
