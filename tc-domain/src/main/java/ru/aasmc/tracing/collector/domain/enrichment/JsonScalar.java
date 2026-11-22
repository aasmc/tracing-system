package ru.aasmc.tracing.collector.domain.enrichment;

import java.math.BigDecimal;

/**
 * A single JSON value read from an enrichment event: a string, a number or a boolean. Objects, arrays and
 * {@code null} are not scalars, so they are never mapped to an attribute.
 */
public sealed interface JsonScalar {

    record StringScalar(String value) implements JsonScalar {}

    /**
     * A JSON number, e.g. {@code 42}, {@code 4.5} or {@code 1e3}, without rounding. As text it reads like
     * {@link BigDecimal#toString()}: {@code 42}, {@code 4.5}, {@code 1E+3}.
     */
    record NumberScalar(BigDecimal value) implements JsonScalar {}

    record BooleanScalar(boolean value) implements JsonScalar {}
}
