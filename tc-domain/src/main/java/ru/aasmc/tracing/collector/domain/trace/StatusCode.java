package ru.aasmc.tracing.collector.domain.trace;

/** OTel span status code, stored in {@code span.status_code smallint} (05 §3.6). */
public enum StatusCode {
    UNSET(0),
    OK(1),
    ERROR(2);

    private final short code;

    StatusCode(int code) {
        this.code = (short) code;
    }

    public short code() {
        return code;
    }

    /**
     * Maps a stored code.
     *
     * @throws IllegalArgumentException for an unknown code
     */
    public static StatusCode fromCode(short code) {
        throw new UnsupportedOperationException();
    }
}
