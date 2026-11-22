package ru.aasmc.tracing.collector.domain.enrichment;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import java.util.Locale;
import java.util.Optional;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.domain.attribute.DigestTransform;

/**
 * Changes a mapped string value before it is stored. Every transform except {@link None} works on strings only.
 * Arguments are checked when the transform is created ({@code INVALID_TRANSFORM}); their size limits are configured in
 * {@link EnrichmentLimits}.
 */
public sealed interface Transform {

    /**
     * Applies the transform.
     *
     * @return the changed value, or empty if the transform cannot produce one (a regular expression did not match)
     */
    default Optional<String> apply(String value) {
        return switch (this) {
            case None _ -> Optional.of(value);
            case Lowercase _ -> Optional.of(value.toLowerCase(Locale.ROOT));
            case Uppercase _ -> Optional.of(value.toUpperCase(Locale.ROOT));
            case Trim _ -> Optional.of(value.strip());
            case Sha256 _ -> Optional.of(DigestTransform.SHA256.apply(value));
            case Mask(int keepLastChars) -> Optional.of(Mask.mask(value, keepLastChars));
            case RegexExtract(String regex) -> RegexExtract.firstGroup(regex, value);
        };
    }

    record None() implements Transform {}

    record Lowercase() implements Transform {}

    record Uppercase() implements Transform {}

    record Trim() implements Transform {}

    /** Replaces the value with its SHA-256 as lowercase hex, so it can still be searched for by equality. */
    record Sha256() implements Transform {}

    /**
     * Replaces all but the last {@code keepLastChars} characters (code points) with {@code *}, e.g. with 4:
     * {@code +79991234567} → {@code ********4567}. A value of at most {@code keepLastChars} characters is unchanged.
     *
     * @param keepLastChars {@code >= 0}
     */
    record Mask(int keepLastChars) implements Transform {

        public Mask {
            require(
                    keepLastChars >= 0,
                    ViolationCode.INVALID_TRANSFORM,
                    "mask keeps a negative number of characters: " + keepLastChars);
        }

        private static String mask(String value, int keepLastChars) {
            int maskedCodePoints = Math.max(0, value.codePointCount(0, value.length()) - keepLastChars);
            return "*".repeat(maskedCodePoints) + value.substring(value.offsetByCodePoints(0, maskedCodePoints));
        }
    }

    /**
     * Keeps the first capturing group of the first match, e.g. {@code code=(\w+)} on {@code "x code=abc y"} gives
     * {@code abc}; no match (or a first group that took no part in the match) gives no value. The expression uses RE2 syntax, whose matching time
     * is linear in the input, so a client-supplied pattern cannot stall the enricher.
     *
     * @param regex a valid RE2 expression with at least one group
     */
    record RegexExtract(String regex) implements Transform {

        public RegexExtract {
            require(regex != null, ViolationCode.INVALID_TRANSFORM, "regex is missing");
            require(capturingGroupCount(regex) >= 1, ViolationCode.INVALID_TRANSFORM, "regex has no group: " + regex);
        }

        private static Optional<String> firstGroup(String regex, String value) {
            Matcher matcher = Pattern.compile(regex).matcher(value);
            return matcher.find() ? Optional.ofNullable(matcher.group(1)) : Optional.empty();
        }

        /** The number of capturing groups, or {@code -1} if {@code regex} is not valid RE2 syntax. */
        private static int capturingGroupCount(String regex) {
            try {
                return Pattern.compile(regex).groupCount();
            } catch (PatternSyntaxException e) {
                return -1;
            }
        }
    }
}
