package ru.aasmc.tracing.collector.domain.enrichment;

/**
 * RFC 6901 JSON Pointer: {@code ""} (whole document) or {@code /a/b~1c} (05 §4.7.1, V3). Evaluated by the enricher
 * with Jackson's {@code JsonNode.at()}; the domain checks only the syntax.
 *
 * <p>Invariant ({@code INVALID_JSON_POINTER}): matches {@code ^(/([^~/]|~[01])*)*$}.
 *
 * @param value the pointer text
 */
public record JsonPointerExpr(String value) {}
