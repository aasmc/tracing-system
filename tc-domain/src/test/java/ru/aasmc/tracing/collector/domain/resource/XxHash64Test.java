package ru.aasmc.tracing.collector.domain.resource;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Expected hashes come from an independent implementation that reproduces the published xxHash64 test vectors (seed 0):
 * {@code ""} → {@code 0xEF46DB3751D8E999}, {@code "a"} → {@code 0xD24EC4F1A98C6E5B}, {@code "abc"} →
 * {@code 0x44BC2CF5AD770999}. Values are printed as signed {@code long}.
 */
class XxHash64Test {

    @ParameterizedTest(name = "\"{0}\"")
    @CsvSource({
        "'', -1205034819632174695",
        "a, -3292477735350538661",
        "abc, 4952883123889572249",
        "Nobody inspects the spammish repetition, -302119147016844303"
    })
    @DisplayName("matches the published xxHash64 test vectors")
    void given_publishedVector_when_hashed_then_publishedValue(String input, long expected) {
        assertThat(XxHash64.hash(input.getBytes(StandardCharsets.UTF_8))).isEqualTo(expected);
    }

    @ParameterizedTest(name = "{0} bytes")
    @CsvSource({
        "0, -1205034819632174695",
        "1, -3292477735350538661",
        "3, 4952883123889572249",
        "4, -2449070131962342708",
        "7, 1756566643212976685",
        "8, 4238821247360054455",
        "15, 3319742962362437736",
        "31, -588808533058512183",
        "32, 5595383772381926855",
        "33, -7294282170313269390",
        "63, -6833289008760248644",
        "64, 1470824913320466200",
        "100, 8775820317742677105"
    })
    @DisplayName("""
            hashes every input length correctly: the 32-byte stripes, the 8-byte and 4-byte tail steps
            and the single trailing bytes (input "abc…z" repeated to the length)""")
    void given_inputLength_when_hashed_then_referenceValue(int length, long expected) {
        StringBuilder input = new StringBuilder();
        for (int i = 0; i < length; i++) {
            input.append((char) ('a' + i % 26));
        }

        assertThat(XxHash64.hash(input.toString().getBytes(StandardCharsets.UTF_8)))
                .isEqualTo(expected);
    }
}
