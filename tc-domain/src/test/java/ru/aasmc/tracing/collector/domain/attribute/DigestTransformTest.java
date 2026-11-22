package ru.aasmc.tracing.collector.domain.attribute;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class DigestTransformTest {

    /** Published SHA-256 test vectors and values of the attribute matrix (expected values from Python hashlib). */
    static Stream<Arguments> sha256Vectors() {
        return Stream.of(
                Arguments.of("", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                Arguments.of("abc", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
                Arguments.of("u-42", "16ec525b74b75626eb93ecad8acc749885fcf4e83c95b139342769a4ab1828a3"),
                Arguments.of("Привет, 世界 🌍", "5cc8b61cd9e69e2d0c6b2064fa57340899b32595cdf5018a39468327fd1fc108"),
                Arguments.of("a\u0000b", "59b271ae1bbcb1d31d41929817f4b16fb439eb4f31520b5ad1d5ce98920a7138"));
    }

    @ParameterizedTest
    @MethodSource("sha256Vectors")
    @DisplayName("SHA256 gives the lower-case hex SHA-256 of the UTF-8 bytes")
    void given_value_when_sha256_then_publishedDigest(String value, String expected) {
        assertThat(DigestTransform.SHA256.apply(value)).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({"u-42, u-***", "ab, ab***", "a, a***", "'', ***", "x🌍yz, x🌍***"})
    @DisplayName("MASK keeps the first two code points and appends ***")
    void given_value_when_mask_then_firstTwoCodePointsKept(String value, String expected) {
        assertThat(DigestTransform.MASK.apply(value)).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "u-42", "Привет"})
    @DisplayName("NONE keeps the value")
    void given_value_when_none_then_unchanged(String value) {
        assertThat(DigestTransform.NONE.apply(value)).isEqualTo(value);
    }
}
