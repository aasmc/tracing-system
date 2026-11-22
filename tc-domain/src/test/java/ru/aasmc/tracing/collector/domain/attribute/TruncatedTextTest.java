package ru.aasmc.tracing.collector.domain.attribute;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TruncatedTextTest {

    @ParameterizedTest(name = "\"{0}\" to {1} bytes")
    @CsvSource({
        "abc,  3, abc,  false",
        "abc,  2, ab,   true",
        "'',   0, '',   false",
        "abc,  0, '',   true",
        "aé,   3, aé,   false",
        "aé,   2, a,    true",
        "a€,   4, a€,   false",
        "a€,   3, a,    true",
        "a🌍,  5, a🌍,  false",
        "a🌍,  4, a,    true",
        "🌍b,  3, '',   true"
    })
    @DisplayName("""
            cuts to at most the given UTF-8 bytes, never in the middle of a character
            (é takes 2 bytes, € 3, 🌍 4), and tells whether anything was removed""")
    void given_textAndLimit_when_truncated_then_wholeCharactersWithinLimit(
            String text, int maxBytes, String expected, boolean wasTruncated) {
        assertThat(TruncatedText.truncate(text, maxBytes)).isEqualTo(new TruncatedText(expected, wasTruncated));
    }
}
