package ru.aasmc.tracing.collector.domain.trace;

/** OTel span kind with its OTLP numeric code, stored in {@code span.kind smallint} (05 §3.6, §6.2). */
public enum SpanKind {
    INTERNAL(1),
    SERVER(2),
    CLIENT(3),
    PRODUCER(4),
    CONSUMER(5);

    private final short code;

    SpanKind(int code) {
        this.code = (short) code;
    }

    public short code() {
        return code;
    }

    /**
     * Maps the wire's enum name ({@code "SERVER"}, ...).
     *
     * @throws IllegalArgumentException for an unknown name
     */
    public static SpanKind fromWire(String name) {
        throw new UnsupportedOperationException();
    }

    /**
     * Maps a stored code.
     *
     * @throws IllegalArgumentException for an unknown code
     */
    public static SpanKind fromCode(short code) {
        throw new UnsupportedOperationException();
    }
}
