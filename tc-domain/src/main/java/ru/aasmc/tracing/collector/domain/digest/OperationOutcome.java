package ru.aasmc.tracing.collector.domain.digest;

/** Business-operation outcome, {@code business_operation_digest.outcome smallint} (05 §4.14.1). */
public enum OperationOutcome {
    UNKNOWN(0),
    SUCCESS(1),
    FAILED(2),
    INCOMPLETE(3);

    private final short code;

    OperationOutcome(int code) {
        this.code = (short) code;
    }

    public short code() {
        return code;
    }

    /**
     * @throws IllegalArgumentException for an unknown code
     */
    public static OperationOutcome ofCode(short code) {
        throw new UnsupportedOperationException();
    }
}
