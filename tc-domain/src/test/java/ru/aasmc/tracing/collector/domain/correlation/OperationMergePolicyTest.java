package ru.aasmc.tracing.collector.domain.correlation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static ru.aasmc.tracing.collector.testmodel.domain.BusinessOperationBuilder.anOperation;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.aasmc.tracing.collector.testmodel.domain.DomainIds;

class OperationMergePolicyTest {

    private static final Instant FIRST = Instant.parse("2026-11-21T09:00:00Z");
    private static final Instant LAST = FIRST.plus(Duration.ofHours(1));
    private static final Duration WINDOW = Duration.ofDays(1);
    private static final OperationMergePolicy POLICY = new OperationMergePolicy(3);

    private final Supplier<UUID> ids = DomainIds.uuidV7Sequence(FIRST);
    private final UUID smallerId = ids.get();
    private final UUID largerId = ids.get();

    @Test
    @DisplayName("an operation with a display value survives a provisional one, even one seen earlier")
    void given_namedAndProvisional_when_survivorChosen_then_named() {
        BusinessOperation provisional =
                anOperation().id(smallerId).seen(FIRST, LAST).displayValue(null).build();
        BusinessOperation named = anOperation().id(largerId).seen(LAST, LAST).build();

        assertThat(POLICY.survivor(provisional, named)).isEqualTo(named);
        assertThat(POLICY.survivor(named, provisional)).isEqualTo(named);
    }

    @Test
    @DisplayName("of two named operations the one first seen earlier survives")
    void given_twoNamed_when_survivorChosen_then_firstSeenEarlier() {
        BusinessOperation earlier = anOperation().id(largerId).seen(FIRST, LAST).build();
        BusinessOperation later = anOperation().id(smallerId).seen(LAST, LAST).build();

        assertThat(POLICY.survivor(earlier, later)).isEqualTo(earlier);
        assertThat(POLICY.survivor(later, earlier)).isEqualTo(earlier);
    }

    @Test
    @DisplayName("of two operations first seen at the same time the smaller id survives")
    void given_sameFirstSeen_when_survivorChosen_then_smallerId() {
        BusinessOperation smaller = anOperation().id(smallerId).build();
        BusinessOperation larger = anOperation().id(largerId).build();

        assertThat(POLICY.survivor(smaller, larger)).isEqualTo(smaller);
        assertThat(POLICY.survivor(larger, smaller)).isEqualTo(smaller);
    }

    @Test
    @DisplayName("a value joins an operation only within the merge window around its seen interval")
    void given_observationTimes_when_canJoin_then_onlyWithinWindow() {
        BusinessOperation operation = anOperation().seen(FIRST, LAST).build();

        assertThat(POLICY.canJoin(operation, FIRST.minus(WINDOW), WINDOW)).isTrue();
        assertThat(POLICY.canJoin(operation, FIRST.minus(WINDOW).minusNanos(1), WINDOW))
                .isFalse();
        assertThat(POLICY.canJoin(operation, LAST.plus(WINDOW), WINDOW)).isTrue();
        assertThat(POLICY.canJoin(operation, LAST.plus(WINDOW).plusNanos(1), WINDOW))
                .isFalse();
    }

    @Test
    @DisplayName("a merge is allowed up to and including the size limit, and refused above it")
    void given_combinedSizes_when_asked_then_allowedUpToLimit() {
        assertThat(POLICY.allowsMerge(3)).isTrue();
        assertThat(POLICY.allowsMerge(4)).isFalse();
    }

    @Test
    @DisplayName("the size limit is at least one trace")
    void given_zeroLimit_when_constructed_then_rejected() {
        assertThatIllegalArgumentException().isThrownBy(() -> new OperationMergePolicy(0));
    }

    @Test
    @DisplayName("""
            Given two named operations first seen at the same time, whose ids differ in the highest bit,
            then the id that is smaller as an unsigned number survives (as PostgreSQL orders uuid),
            although Java's signed UUID.compareTo would order them the other way""")
    void given_idsDifferingInHighestBit_when_survivorChosen_then_unsignedOrder() {
        UUID lowId = new UUID(0x0000_0000_0001_7000L, 0x8000_0000_0000_0001L);
        UUID highId = new UUID(0xF000_0000_0001_7000L, 0x8000_0000_0000_0001L);
        BusinessOperation low = anOperation().id(lowId).build();
        BusinessOperation high = anOperation().id(highId).build();

        assertThat(highId.compareTo(lowId)).isNegative();
        assertThat(POLICY.survivor(high, low)).isEqualTo(low);
        assertThat(POLICY.survivor(low, high)).isEqualTo(low);
    }
}
