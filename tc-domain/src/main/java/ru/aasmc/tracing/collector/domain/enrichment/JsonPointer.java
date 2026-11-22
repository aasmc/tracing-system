package ru.aasmc.tracing.collector.domain.enrichment;

/**
 * A JSON Pointer (RFC 6901): {@code ""} for the whole document or a path such as {@code /a/b~1c}, where {@code ~1}
 * stands for {@code /} and {@code ~0} for {@code ~}.
 *
 * <p>The syntax is checked ({@code INVALID_JSON_POINTER}); whether the path exists is known only per event.
 *
 * @param value the pointer text
 */
public record JsonPointer(String value) {}
