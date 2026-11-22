package ru.aasmc.tracing.collector.domain.trace;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.testmodel.domain.DomainIds;

class SpanIdTest {

    private static final String HEX = "00f067aa0ba902b7";

    @ParameterizedTest
    @CsvSource({
        "0000000000000001, 1",
        "7fffffffffffffff, 9223372036854775807",
        "8000000000000000, -9223372036854775808",
        "ffffffffffffffff, -1"
    })
    @DisplayName("maps hex to the signed long PostgreSQL stores, and back without loss")
    void given_hex_when_parsedAndPrinted_then_signedLongRoundTrip(String hex, long stored) {
        assertThat(SpanId.parse(hex)).isEqualTo(new SpanId(stored));
        assertThat(new SpanId(stored).toHex()).isEqualTo(hex);
        assertThat(new SpanId(stored).toString()).isEqualTo(hex);
    }

    @Test
    @DisplayName("accepts upper-case hex")
    void given_upperCaseHex_when_parse_then_sameId() {
        assertThat(SpanId.parse("00F067AA0BA902B7")).isEqualTo(DomainIds.spanId(HEX));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(
            strings = {
                "00f067aa0ba902b",
                "00f067aa0ba902b70",
                "00f067aa0ba902bg",
                "+0f067aa0ba902b7",
                "0000000000000000"
            })
    @DisplayName("rejects a wrong length, a non-hex character and the all-zero id")
    void given_invalidHex_when_parse_then_invalidSpanId(String hex) {
        assertViolation(ViolationCode.INVALID_SPAN_ID, () -> SpanId.parse(hex));
    }

    @Test
    @DisplayName("rejects zero when constructed from bits")
    void given_zero_when_constructed_then_invalidSpanId() {
        assertViolation(ViolationCode.INVALID_SPAN_ID, () -> new SpanId(0));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = "0000000000000000")
    @DisplayName("a root span's parent id is absent")
    void given_rootParentId_when_parseParent_then_absent(String hex) {
        assertThat(SpanId.parseParent(hex)).isEmpty();
    }

    @Test
    @DisplayName("parses any other parent id like a span id")
    void given_parentId_when_parseParent_then_parsedLikeSpanId() {
        assertThat(SpanId.parseParent(HEX)).contains(DomainIds.spanId(HEX));
        assertViolation(ViolationCode.INVALID_SPAN_ID, () -> SpanId.parseParent("00f067aa0ba902bg"));
    }
}
