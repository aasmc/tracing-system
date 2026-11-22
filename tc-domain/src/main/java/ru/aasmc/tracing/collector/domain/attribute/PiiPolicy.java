package ru.aasmc.tracing.collector.domain.attribute;

import java.util.List;
import java.util.Map;

/**
 * Classification of attribute keys into {@link PiiClass} (05 §7.3), configured in {@code tc-query-api} and
 * {@code tc-control-plane} through {@code tc.security.pii.keys}.
 *
 * <p>Rules: an exact key wins; otherwise the first matching glob in declaration order; otherwise {@code NONE}. A glob
 * may contain {@code *} (any characters, including dots) and is matched against the whole key.
 *
 * @param exact exact keys and their classes
 * @param globs glob patterns in priority order
 */
public record PiiPolicy(Map<String, PiiClass> exact, List<KeyGlob> globs) {

    /**
     * Baseline of 05 §7.3: {@code user_id}, {@code enduser.id}, {@code phone}, {@code *.phone}, {@code email},
     * {@code *.email} are PERSONAL; {@code oauth.authorization_code}, {@code *.token}, {@code *.password},
     * {@code *.secret} are SECRET.
     */
    public static PiiPolicy baseline() {
        throw new UnsupportedOperationException();
    }

    /**
     * Builds a policy from configuration entries; entries containing {@code *} become globs (in map order), the rest
     * exact keys.
     */
    public static PiiPolicy of(Map<String, PiiClass> keysAndGlobs) {
        throw new UnsupportedOperationException();
    }

    public PiiClass classify(AttributeKey key) {
        throw new UnsupportedOperationException();
    }

    /**
     * One glob rule.
     *
     * @param glob pattern with {@code *} wildcards
     * @param piiClass class of matching keys
     */
    public record KeyGlob(String glob, PiiClass piiClass) {

        public boolean matches(AttributeKey key) {
            throw new UnsupportedOperationException();
        }
    }
}
