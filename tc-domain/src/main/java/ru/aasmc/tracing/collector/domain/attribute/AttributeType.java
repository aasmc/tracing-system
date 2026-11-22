package ru.aasmc.tracing.collector.domain.attribute;

/** The eight OTel attribute types, with the names the starter writes on the wire (05 §3.4). */
public enum AttributeType {
    STRING,
    BOOLEAN,
    LONG,
    DOUBLE,
    STRING_ARRAY,
    BOOLEAN_ARRAY,
    LONG_ARRAY,
    DOUBLE_ARRAY;

    public boolean isArray() {
        throw new UnsupportedOperationException();
    }

    /** {@code true} for LONG, DOUBLE and their arrays. */
    public boolean isNumeric() {
        throw new UnsupportedOperationException();
    }
}
