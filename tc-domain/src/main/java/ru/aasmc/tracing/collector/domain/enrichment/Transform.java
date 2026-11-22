package ru.aasmc.tracing.collector.domain.enrichment;

/**
 * Changes a mapped string value before it is stored. Every transform except {@link None} works on strings only.
 * Arguments are checked when the transform is created ({@code INVALID_TRANSFORM}).
 */
public sealed interface Transform {

    record None() implements Transform {}

    record Lowercase() implements Transform {}

    record Uppercase() implements Transform {}

    record Trim() implements Transform {}

    /** Replaces the value with its SHA-256 as lowercase hex, so it can still be searched for by equality. */
    record Sha256() implements Transform {}

    /**
     * Replaces all but the last {@code keepLastChars} characters with {@code *}.
     *
     * @param keepLastChars 0..{@value #MAX_KEEP_LAST_CHARS}
     */
    record Mask(int keepLastChars) implements Transform {

        public static final int MAX_KEEP_LAST_CHARS = 16;
    }

    /**
     * Keeps the first capturing group of a regular expression. The expression uses RE2 syntax, whose matching time
     * is linear in the input, so a client-supplied pattern cannot stall the enricher.
     *
     * @param regex a valid RE2 expression with at least one group, at most {@value #MAX_REGEX_LENGTH} characters
     */
    record RegexExtract(String regex) implements Transform {

        public static final int MAX_REGEX_LENGTH = 256;
    }
}
