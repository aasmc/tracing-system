package ru.aasmc.tracing.collector.domain.enrichment;

import com.google.re2j.Pattern;
import java.util.Optional;

/**
 * Value transform of a field mapping (05 §4.7.1, 03 §5.4 V9). Every transform except {@link None} works on strings
 * and yields a STRING.
 *
 * <p>Persisted as the pair {@code (transform, transformArg)}; {@link #of(String, String)} and {@link #code()} /
 * {@link #argument()} convert both ways.
 */
public sealed interface Transform {

    /** Persisted name: NONE, LOWERCASE, UPPERCASE, TRIM, SHA256, MASK, REGEX_EXTRACT. */
    String code();

    /** Persisted argument, or {@code null}. */
    String argument();

    /** {@code false} only for {@link None}. */
    boolean stringOnly();

    /** Applies the transform; empty when it yields no value (only {@link RegexExtract} without a match). */
    Optional<String> apply(String value);

    /**
     * Builds a transform from its persisted pair; a {@code null} name is NONE.
     *
     * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_TRANSFORM} for an unknown
     *     name or a bad argument
     */
    static Transform of(String name, String argument) {
        throw new UnsupportedOperationException();
    }

    /** Identity. */
    record None() implements Transform {

        @Override
        public String code() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String argument() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean stringOnly() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<String> apply(String value) {
            throw new UnsupportedOperationException();
        }
    }

    /** {@link String#toLowerCase(java.util.Locale)} with {@code Locale.ROOT}. */
    record Lowercase() implements Transform {

        @Override
        public String code() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String argument() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean stringOnly() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<String> apply(String value) {
            throw new UnsupportedOperationException();
        }
    }

    /** {@link String#toUpperCase(java.util.Locale)} with {@code Locale.ROOT}. */
    record Uppercase() implements Transform {

        @Override
        public String code() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String argument() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean stringOnly() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<String> apply(String value) {
            throw new UnsupportedOperationException();
        }
    }

    /** {@link String#strip()}. */
    record Trim() implements Transform {

        @Override
        public String code() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String argument() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean stringOnly() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<String> apply(String value) {
            throw new UnsupportedOperationException();
        }
    }

    /** Lowercase hex SHA-256 of the UTF-8 value: PII that must stay joinable but not readable. */
    record Sha256() implements Transform {

        @Override
        public String code() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String argument() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean stringOnly() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<String> apply(String value) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Keeps the last {@code keepLast} code points and replaces the others with {@code *}. Invariant: {@code 0 <=
     * keepLast <= 16} (V9; deviation from 05, which said 0..8); default 2.
     *
     * @param keepLast code points kept at the end
     */
    record Mask(int keepLast) implements Transform {

        public static final int DEFAULT_KEEP_LAST = 2;
        public static final int MAX_KEEP_LAST = 16;

        @Override
        public String code() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String argument() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean stringOnly() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<String> apply(String value) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Extracts the single capturing group of an RE2/J regular expression (linear time, no catastrophic
     * backtracking). No match yields no value. Invariants (V9): at most {@link #MAX_REGEX_LENGTH} characters, compiles
     * with RE2/J, exactly one capturing group. Equality is by regex text.
     */
    final class RegexExtract implements Transform {

        public static final int MAX_REGEX_LENGTH = 256;

        private final String regex;
        private final Pattern pattern;

        /**
         * @throws ru.aasmc.tracing.collector.domain.DomainViolationException {@code INVALID_TRANSFORM}
         */
        public RegexExtract(String regex) {
            this.regex = regex;
            this.pattern = null;
            throw new UnsupportedOperationException();
        }

        public String regex() {
            return regex;
        }

        Pattern pattern() {
            return pattern;
        }

        @Override
        public String code() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String argument() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean stringOnly() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<String> apply(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int hashCode() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            throw new UnsupportedOperationException();
        }
    }
}
