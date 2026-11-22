package ru.aasmc.tracing.collector.domain.enrichment;

import static ru.aasmc.tracing.collector.domain.DomainViolationException.require;

import java.util.regex.Pattern;
import ru.aasmc.tracing.collector.domain.ViolationCode;

/**
 * A JSON Pointer (RFC 6901): {@code ""} for the whole document or a path such as {@code /a/b~1c}, where {@code ~1}
 * stands for {@code /} and {@code ~0} for {@code ~}.
 *
 * <p>The syntax is checked ({@code INVALID_JSON_POINTER}); whether the path exists is known only per event.
 *
 * @param value the pointer text
 */
public record JsonPointer(String value) {

    /** Reference tokens each start with {@code /}; {@code ~} only appears as {@code ~0} or {@code ~1}. */
    private static final Pattern SYNTAX = Pattern.compile("(/([^~/]|~[01])*)*");

    public JsonPointer {
        require(
                value != null && SYNTAX.matcher(value).matches(),
                ViolationCode.INVALID_JSON_POINTER,
                "not a JSON Pointer: " + value);
    }
}
