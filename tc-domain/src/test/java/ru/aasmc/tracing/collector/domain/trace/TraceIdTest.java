package ru.aasmc.tracing.collector.domain.trace;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.testmodel.domain.DomainIds;

class TraceIdTest {

    private static final String HEX = "4bf92f3577b34da6a3ce929d0e0e4736";

    @Test
    @DisplayName("parses lower- and upper-case hex into the same 128 bits")
    void given_hexInEitherCase_when_parse_then_sameId() {
        TraceId expected = DomainIds.traceId(HEX);

        assertThat(TraceId.parse(HEX)).isEqualTo(expected);
        assertThat(TraceId.parse(HEX.toUpperCase(Locale.ROOT))).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"0000000000000001000000000000000f", "ffffffffffffffff8000000000000000"})
    @DisplayName("prints 32 lower-case hex characters, keeping leading zeros")
    void given_id_when_toHex_then_originalHex(String hex) {
        TraceId id = DomainIds.traceId(hex);

        assertThat(id.toHex()).isEqualTo(hex);
        assertThat(id.toString()).isEqualTo(hex);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(
            strings = {
                "4bf92f3577b34da6a3ce929d0e0e473",
                "4bf92f3577b34da6a3ce929d0e0e47361",
                "4bf92f3577b34da6a3ce929d0e0e473g",
                "+bf92f3577b34da6a3ce929d0e0e4736",
                "00000000000000000000000000000000"
            })
    @DisplayName("rejects a wrong length, a non-hex character and the all-zero id")
    void given_invalidHex_when_parse_then_invalidTraceId(String hex) {
        assertViolation(ViolationCode.INVALID_TRACE_ID, () -> TraceId.parse(hex));
    }

    @Test
    @DisplayName("rejects the all-zero id when constructed from bits")
    void given_zeroBits_when_constructed_then_invalidTraceId() {
        assertViolation(ViolationCode.INVALID_TRACE_ID, () -> new TraceId(0, 0));
    }

    @Test
    @DisplayName("converts to and from a UUID with the same 128 bits")
    void given_id_when_convertedToUuidAndBack_then_sameBits() {
        TraceId id = DomainIds.traceId(HEX);
        UUID uuid = UUID.fromString("4bf92f35-77b3-4da6-a3ce-929d0e0e4736");

        assertThat(id.toUuid()).isEqualTo(uuid);
        assertThat(TraceId.fromUuid(uuid)).isEqualTo(id);
    }

    @Test
    @DisplayName("orders ids as unsigned bytes, the order PostgreSQL uses for uuid")
    void given_ids_when_sorted_then_unsignedByteOrder() {
        List<TraceId> ascending = List.of(
                        "0".repeat(31) + "1",
                        "0".repeat(16) + "f".repeat(16),
                        "7" + "f".repeat(15) + "0".repeat(16),
                        "8" + "0".repeat(31),
                        "f".repeat(32))
                .stream()
                .map(DomainIds::traceId)
                .toList();

        assertThat(ascending.reversed().stream().sorted().toList()).isEqualTo(ascending);
    }
}
