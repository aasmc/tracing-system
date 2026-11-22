package ru.aasmc.tracing.collector.domain;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.util.regex.Pattern;

/**
 * The naming rule for things an admin creates, such as enrichment sources and business operation definitions: at least 3
 * characters of {@code a-z 0-9 . _ -}, starting with a letter or digit. Such names become part of Kafka consumer group
 * ids, so they are free of characters that need quoting. The maximum length is configured per kind of thing.
 */
public final class Names {

    private static final Pattern VALID_NAME = Pattern.compile("[a-z0-9][a-z0-9._-]{2,}");

    private Names() {}

    /** @throws DomainViolationException {@code INVALID_NAME} if {@code name} breaks the rule */
    public static void requireValid(String name) {
        require(
                name != null && VALID_NAME.matcher(name).matches(),
                ViolationCode.INVALID_NAME,
                "invalid name: " + name);
    }

    /** @throws DomainViolationException {@code INVALID_NAME} if {@code name} is longer than {@code maxLength} */
    public static void requireAtMost(String name, int maxLength) {
        require(
                name.length() <= maxLength,
                ViolationCode.INVALID_NAME,
                "name longer than " + maxLength + " characters: " + name);
    }
}
