package ru.aasmc.tracing.collector.domain.attribute;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Decides which attribute keys hold personal data or secrets, and how their values are shown.
 *
 * <p>A rule's key is either an exact attribute key ({@code user_id}) or a pattern with {@code *} matching any
 * characters ({@code *.phone}). An exact rule wins over a pattern; of several matching patterns the most sensitive
 * one wins; keys without a rule are {@link Sensitivity#NONE}. Patterns are compiled once, when the policy is built.
 */
public final class SensitivityPolicy {

    private static final String WILDCARD = "*";

    private final Map<String, Sensitivity> exactRules;
    private final List<PatternRule> patternRules;

    /** @param rules exact keys or patterns, each with the sensitivity of the matching keys */
    public SensitivityPolicy(Map<String, Sensitivity> rules) {
        exactRules = rules.entrySet().stream()
                .filter(rule -> !rule.getKey().contains(WILDCARD))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
        patternRules = rules.entrySet().stream()
                .filter(rule -> rule.getKey().contains(WILDCARD))
                .map(rule -> new PatternRule(compile(rule.getKey()), rule.getValue()))
                .toList();
    }

    /** How sensitive the values of {@code key} are, by the rules described above. */
    public Sensitivity sensitivityOf(AttributeKey key) {
        Sensitivity exact = exactRules.get(key.value());
        if (exact != null) {
            return exact;
        }
        return patternRules.stream()
                .filter(rule -> rule.pattern().matcher(key.value()).matches())
                .map(PatternRule::sensitivity)
                .max(Comparator.naturalOrder())
                .orElse(Sensitivity.NONE);
    }

    /**
     * The value as a caller may see it: SECRET values are always masked; PERSONAL values are masked unless the caller
     * may see personal data; other values are shown as they are. Masking works like {@link DigestTransform#MASK}.
     */
    public String displayValue(AttributeKey key, String value, boolean callerMaySeePersonalData) {
        boolean masked = switch (sensitivityOf(key)) {
            case NONE -> false;
            case PERSONAL -> !callerMaySeePersonalData;
            case SECRET -> true;
        };
        return masked ? DigestTransform.MASK.apply(value) : value;
    }

    /** {@code *.phone} becomes a regex matching any text that ends with {@code .phone}; other characters are literal. */
    private static Pattern compile(String wildcardPattern) {
        return Pattern.compile(Arrays.stream(wildcardPattern.split(Pattern.quote(WILDCARD), -1))
                .map(Pattern::quote)
                .collect(Collectors.joining(".*")));
    }

    private record PatternRule(Pattern pattern, Sensitivity sensitivity) {}
}
