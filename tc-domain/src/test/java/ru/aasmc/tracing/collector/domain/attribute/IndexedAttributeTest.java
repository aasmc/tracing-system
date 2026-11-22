package ru.aasmc.tracing.collector.domain.attribute;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.aasmc.tracing.collector.domain.attribute.DigestTransform.NONE;
import static ru.aasmc.tracing.collector.domain.attribute.DigestTransform.SHA256;
import static ru.aasmc.tracing.collector.domain.attribute.IndexStatus.ACTIVE;
import static ru.aasmc.tracing.collector.domain.attribute.IndexStatus.BACKFILLING;
import static ru.aasmc.tracing.collector.domain.attribute.IndexStatus.DISABLED;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.testmodel.domain.Transitions;

class IndexedAttributeTest {

    @ParameterizedTest
    @CsvSource({
        "BACKFILLING, BACKFILLING, true",
        "BACKFILLING, ACTIVE,      true",
        "BACKFILLING, DISABLED,    true",
        "ACTIVE,      BACKFILLING, false",
        "ACTIVE,      ACTIVE,      true",
        "ACTIVE,      DISABLED,    true",
        "DISABLED,    BACKFILLING, true",
        "DISABLED,    ACTIVE,      false",
        "DISABLED,    DISABLED,    true"
    })
    @DisplayName("a key is backfilled before it is active, can always be disabled and is enabled again by backfilling")
    void given_statuses_when_transitionTo_then_allowedOrRejected(IndexStatus from, IndexStatus to, boolean allowed) {
        Transitions.assertTransition(from.transitionTo(to), from, to, allowed, ViolationCode.INVALID_INDEX_TRANSITION);
    }

    @Test
    @DisplayName("moving to an allowed status changes only the status")
    void given_backfillingKey_when_movedToActive_then_active() {
        assertThat(indexed(BACKFILLING, false, false, NONE).moveTo(ACTIVE))
                .isEqualTo(indexed(ACTIVE, false, false, NONE));
    }

    @Test
    @DisplayName("refuses a forbidden status change, disabling a built-in key and disabling a retained key")
    void given_forbiddenMove_when_moveTo_then_rejected() {
        assertViolation(
                ViolationCode.INVALID_INDEX_TRANSITION,
                () -> indexed(ACTIVE, false, false, NONE).moveTo(BACKFILLING));
        assertViolation(
                ViolationCode.BUILT_IN_KEY,
                () -> indexed(ACTIVE, true, false, NONE).moveTo(DISABLED));
        assertViolation(
                ViolationCode.RETAINED_KEY_MUST_BE_INDEXED,
                () -> indexed(ACTIVE, false, true, SHA256).moveTo(DISABLED));
    }

    @Test
    @DisplayName(
            "rejects a disabled built-in key, a disabled retained key and a transform on a key that is not retained")
    void given_inconsistentFields_when_constructed_then_rejected() {
        assertViolation(ViolationCode.BUILT_IN_KEY, () -> indexed(DISABLED, true, false, NONE));
        assertViolation(ViolationCode.RETAINED_KEY_MUST_BE_INDEXED, () -> indexed(DISABLED, false, true, NONE));
        assertViolation(ViolationCode.INVALID_DIGEST_TRANSFORM, () -> indexed(ACTIVE, false, false, SHA256));
    }

    @Test
    @DisplayName("personal data is retained hashed, or in clear text only when acknowledged")
    void given_personalData_when_retained_then_hashedOrAcknowledged() {
        IndexedAttribute key = indexed(ACTIVE, false, false, NONE);

        assertThat(key.retainInDigest(SHA256, Sensitivity.PERSONAL, false))
                .isEqualTo(indexed(ACTIVE, false, true, SHA256));
        assertThat(key.retainInDigest(NONE, Sensitivity.PERSONAL, true)).isEqualTo(indexed(ACTIVE, false, true, NONE));
        assertViolation(
                ViolationCode.PERSONAL_DATA_RETAINED_IN_CLEAR,
                () -> key.retainInDigest(NONE, Sensitivity.PERSONAL, false));
    }

    @Test
    @DisplayName("secrets are never retained in clear text, and a disabled key is never retained")
    void given_secretOrDisabledKey_when_retainedInClear_then_rejected() {
        assertViolation(
                ViolationCode.SECRET_RETAINED_IN_CLEAR,
                () -> indexed(ACTIVE, false, false, NONE).retainInDigest(NONE, Sensitivity.SECRET, true));
        assertViolation(
                ViolationCode.RETAINED_KEY_MUST_BE_INDEXED,
                () -> indexed(DISABLED, false, false, NONE).retainInDigest(SHA256, Sensitivity.NONE, false));
    }

    private static IndexedAttribute indexed(
            IndexStatus status, boolean builtIn, boolean retained, DigestTransform transform) {
        return new IndexedAttribute(7, new AttributeKey("user_id"), status, builtIn, retained, transform);
    }

    @Test
    @DisplayName("""
            secrets may be retained hashed or masked (never in clear text),
            and personal data may be retained masked without an acknowledgement""")
    void given_hashedOrMaskedTransform_when_retained_then_allowedWithoutAcknowledgement() {
        IndexedAttribute key = indexed(ACTIVE, false, false, NONE);

        assertThat(key.retainInDigest(SHA256, Sensitivity.SECRET, false))
                .isEqualTo(indexed(ACTIVE, false, true, SHA256));
        assertThat(key.retainInDigest(DigestTransform.MASK, Sensitivity.SECRET, false))
                .isEqualTo(indexed(ACTIVE, false, true, DigestTransform.MASK));
        assertThat(key.retainInDigest(DigestTransform.MASK, Sensitivity.PERSONAL, false))
                .isEqualTo(indexed(ACTIVE, false, true, DigestTransform.MASK));
    }
}
