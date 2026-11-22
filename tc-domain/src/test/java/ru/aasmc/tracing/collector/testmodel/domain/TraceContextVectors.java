package ru.aasmc.tracing.collector.testmodel.domain;

import java.util.List;
import java.util.Locale;

/** {@code traceparent} values and raw trace-id values with the expected parse result. */
public final class TraceContextVectors {

    public static final String TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";
    public static final String SPAN = "00f067aa0ba902b7";
    private static final String VALID = "00-" + TRACE + "-" + SPAN + "-01";

    private TraceContextVectors() {}

    /**
     * A value and the expected result; {@code version} is {@code -1} for an invalid value.
     *
     * @param flags expected flags byte
     */
    public record Vector(String name, String value, int version, int flags) {

        public boolean valid() {
            return version >= 0;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /** All valid vectors carry trace {@link #TRACE} and parent {@link #SPAN}. */
    public static List<Vector> traceparents() {
        return List.of(
                valid("sampled", VALID, 0, 0x01),
                valid("not sampled", "00-" + TRACE + "-" + SPAN + "-00", 0, 0x00),
                valid("unknown flag bits are kept", "00-" + TRACE + "-" + SPAN + "-ff", 0, 0xff),
                valid("upper case", VALID.toUpperCase(Locale.ROOT), 0, 0x01),
                valid("future version", "01-" + TRACE + "-" + SPAN + "-01", 1, 0x01),
                valid("future version with extra fields", "cc-" + TRACE + "-" + SPAN + "-01-later-fields", 0xcc, 0x01),
                invalid("version ff", "ff-" + TRACE + "-" + SPAN + "-01"),
                invalid("version 00 with extra fields", VALID + "-extra"),
                invalid("future version, 56th character not '-'", "01-" + TRACE + "-" + SPAN + "-01x"),
                invalid("all-zero trace id", "00-" + "0".repeat(32) + "-" + SPAN + "-01"),
                invalid("all-zero parent id", "00-" + TRACE + "-" + "0".repeat(16) + "-01"),
                invalid("trace id too short", "00-" + TRACE.substring(1) + "-" + SPAN + "-01"),
                invalid("span id too short", "00-" + TRACE + "-" + SPAN.substring(1) + "-01"),
                invalid("flags too short", "00-" + TRACE + "-" + SPAN + "-1"),
                invalid("non-hex character", "00-g" + TRACE.substring(1) + "-" + SPAN + "-01"),
                invalid("wrong separator", "00_" + TRACE + "_" + SPAN + "_01"),
                invalid("missing separator after the trace id", "00-" + TRACE + "x" + SPAN + "-01"),
                invalid("missing separator after the parent id", "00-" + TRACE + "-" + SPAN + "x01"),
                invalid("non-hex version", "0g-" + TRACE + "-" + SPAN + "-01"),
                invalid("non-hex flags", "00-" + TRACE + "-" + SPAN + "-0g"),
                invalid("empty", ""),
                invalid("null", null));
    }

    private static Vector valid(String name, String value, int version, int flags) {
        return new Vector(name, value, version, flags);
    }

    private static Vector invalid(String name, String value) {
        return new Vector(name, value, -1, 0);
    }
}
