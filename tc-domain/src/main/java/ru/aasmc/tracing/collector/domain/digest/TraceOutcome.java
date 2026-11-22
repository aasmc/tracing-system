package ru.aasmc.tracing.collector.domain.digest;

/** Trace-level outcome, {@code trace_digest.outcome smallint} (05 §4.14.1). */
public enum TraceOutcome {
    OK(1),
    ERROR(2);

    private final short code;

    TraceOutcome(int code) {
        this.code = (short) code;
    }

    public short code() {
        return code;
    }

    /** ERROR iff {@code errorCount > 0}. */
    public static TraceOutcome ofErrorCount(int errorCount) {
        throw new UnsupportedOperationException();
    }

    /**
     * @throws IllegalArgumentException for an unknown code
     */
    public static TraceOutcome ofCode(short code) {
        throw new UnsupportedOperationException();
    }
}
