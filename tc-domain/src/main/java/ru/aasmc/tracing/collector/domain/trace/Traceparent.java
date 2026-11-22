package ru.aasmc.tracing.collector.domain.trace;

import java.util.Optional;

/**
 * A W3C {@code traceparent} value: {@code version-traceid-parentid-flags}, e.g.
 * {@code 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01}.
 *
 * @param version the version byte (0..254)
 * @param traceId the trace id
 * @param parentSpanId the span that sent the message
 * @param flags the trace flags
 */
public record Traceparent(int version, TraceId traceId, SpanId parentSpanId, TraceFlags flags) {

    /**
     * Length of {@code vv-<32 hex trace id>-<16 hex parent id>-<2 hex flags>}; the fields start at indexes 0, 3, 36
     * and 53.
     */
    private static final int VERSION_00_LENGTH = 55;

    private static final int INVALID_VERSION = 0xff;

    /**
     * Parses a value; never throws. Rules:
     *
     * <ul>
     *   <li>hex may be upper or lower case;
     *   <li>version {@code ff} is invalid;
     *   <li>version {@code 00} has exactly 55 characters;
     *   <li>a higher (future) version is valid if its first 55 characters parse and the 56th character, if any, is
     *       {@code '-'}: later versions may append fields;
     *   <li>an all-zero trace id or parent id is invalid.
     * </ul>
     *
     * @return the parsed value, or empty if {@code value} is null or invalid
     */
    public static Optional<Traceparent> parse(String value) {
        if (value == null
                || value.length() < VERSION_00_LENGTH
                || !Hex.isHex(value, 0, 2)
                || value.charAt(2) != '-'
                || value.charAt(35) != '-'
                || value.charAt(52) != '-'
                || !Hex.isHex(value, 53, VERSION_00_LENGTH)) {
            return Optional.empty();
        }
        int version = Integer.parseInt(value, 0, 2, 16);
        boolean validLength = version == 0
                ? value.length() == VERSION_00_LENGTH
                : value.length() == VERSION_00_LENGTH || value.charAt(VERSION_00_LENGTH) == '-';
        if (version == INVALID_VERSION || !validLength) {
            return Optional.empty();
        }
        TraceFlags flags = new TraceFlags(Integer.parseInt(value, 53, VERSION_00_LENGTH, 16));
        return TraceId.tryParse(value.substring(3, 35))
                .flatMap(traceId -> SpanId.tryParse(value.substring(36, 52))
                        .map(parentSpanId -> new Traceparent(version, traceId, parentSpanId, flags)));
    }
}
