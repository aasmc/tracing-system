package ru.aasmc.tracing.collector.domain.attribute;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class SensitivityPolicyTest {

    private static final SensitivityPolicy POLICY = new SensitivityPolicy(Map.of(
            "user_id", Sensitivity.PERSONAL,
            "*.phone", Sensitivity.PERSONAL,
            "oauth.*", Sensitivity.SECRET,
            "oauth.client_id", Sensitivity.NONE));

    @ParameterizedTest
    @CsvSource({
        "user_id,                  PERSONAL",
        "customer.phone,           PERSONAL",
        "oauth.authorization_code, SECRET",
        "oauth.client_id,          NONE",
        "client_id,                NONE",
        "phone,                    NONE"
    })
    @DisplayName("classifies keys by exact rule first, then by pattern, else as not sensitive")
    void given_key_when_classified_then_expectedSensitivity(String key, Sensitivity expected) {
        assertThat(POLICY.sensitivityOf(new AttributeKey(key))).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({
        "user_id,                  u-42,      false, u-***",
        "user_id,                  u-42,      true,  u-42",
        "user_id,                  16ec525b,  false, 16***",
        "user_id,                  16ec525b,  true,  16ec525b",
        "oauth.authorization_code, code-abc,  false, co***",
        "oauth.authorization_code, code-abc,  true,  co***",
        "client_id,                bookstore, false, bookstore"
    })
    @DisplayName("personal values are shown only to callers allowed to see them, secrets never, others always")
    void given_value_when_displayed_then_maskedOrShown(
            String key, String value, boolean callerMaySeePersonalData, String expected) {
        assertThat(POLICY.displayValue(new AttributeKey(key), value, callerMaySeePersonalData))
                .isEqualTo(expected);
    }

    @Test
    @DisplayName("""
            Given a key matched by two patterns of different classes (*.phone PERSONAL, customer.* SECRET),
            then the most sensitive class wins: customer.phone is SECRET""")
    void given_keyMatchingSeveralPatterns_when_classified_then_mostSensitiveWins() {
        SensitivityPolicy policy =
                new SensitivityPolicy(Map.of("*.phone", Sensitivity.PERSONAL, "customer.*", Sensitivity.SECRET));

        assertThat(policy.sensitivityOf(new AttributeKey("customer.phone"))).isEqualTo(Sensitivity.SECRET);
    }

    @ParameterizedTest
    @CsvSource({
        "user.billing.id, PERSONAL",
        "user..id,        PERSONAL",
        "user.id,         NONE",
        "userXbillingXid, NONE"
    })
    @DisplayName("""
            * matches any characters, also in the middle of a pattern (user.*.id),
            and every other character, including '.', matches only itself""")
    void given_wildcardInTheMiddle_when_classified_then_otherCharactersLiteral(String key, Sensitivity expected) {
        SensitivityPolicy policy = new SensitivityPolicy(Map.of("user.*.id", Sensitivity.PERSONAL));

        assertThat(policy.sensitivityOf(new AttributeKey(key))).isEqualTo(expected);
    }
}
