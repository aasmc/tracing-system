package ru.aasmc.tracing.collector.domain.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.aasmc.tracing.collector.domain.ViolationCode;

class RetentionTiersTest {

    @Test
    @DisplayName("full data is kept at least two days")
    void given_fullDataDays_when_constructed_then_atLeastTwo() {
        assertThat(new RetentionTiers(2, 90).fullDataRetention()).isEqualTo(Duration.ofDays(2));
        assertViolation(ViolationCode.FULL_DATA_DAYS_TOO_SMALL, () -> new RetentionTiers(1, 90));
    }

    @Test
    @DisplayName("digest days are zero (no digest) or more")
    void given_digestDays_when_constructed_then_notNegative() {
        assertThat(new RetentionTiers(7, 0).digestDays()).isZero();
        assertViolation(ViolationCode.INVALID_DIGEST_DAYS, () -> new RetentionTiers(7, -1));
    }

    @Test
    @DisplayName("full data outlives the merge window by at least a day")
    void given_mergeWindow_when_checked_then_atMostFullDataDaysMinusOne() {
        RetentionTiers tiers = new RetentionTiers(7, 90);

        assertThatNoException().isThrownBy(() -> tiers.requireFullDataOutlives(Duration.ofDays(6)));
        assertViolation(
                ViolationCode.FULL_DATA_DAYS_BELOW_MERGE_WINDOW,
                () -> tiers.requireFullDataOutlives(Duration.ofDays(6).plusSeconds(1)));
    }
}
