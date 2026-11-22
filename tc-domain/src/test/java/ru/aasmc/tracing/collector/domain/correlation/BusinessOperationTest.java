package ru.aasmc.tracing.collector.domain.correlation;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.aasmc.tracing.collector.testmodel.domain.BusinessOperationBuilder.anOperation;
import static ru.aasmc.tracing.collector.testmodel.domain.Violations.assertViolation;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import ru.aasmc.tracing.collector.domain.ViolationCode;
import ru.aasmc.tracing.collector.testmodel.domain.DomainIds;

class BusinessOperationTest {

    private static final Instant FIRST = Instant.parse("2026-11-21T09:00:00Z");
    private static final Instant LAST = FIRST.plus(Duration.ofHours(1));

    private final Supplier<UUID> ids = DomainIds.uuidV7Sequence(FIRST);

    @Test
    @DisplayName("an operation without a display value is provisional")
    void given_displayValueOrNot_when_asked_then_provisionalOnlyWithout() {
        assertThat(anOperation().displayValue(null).build().isProvisional()).isTrue();
        assertThat(anOperation().displayValue("as-777").build().isProvisional()).isFalse();
    }

    @Test
    @DisplayName("an observation widens the seen interval and names a provisional operation")
    void given_observations_when_included_then_boundsWidenedAndNameSetOnce() {
        BusinessOperation provisional =
                anOperation().seen(FIRST, LAST).displayValue(null).build();

        BusinessOperation earlier = provisional.includeObservation(FIRST.minusSeconds(60), null);
        BusinessOperation named = provisional.includeObservation(LAST.plusSeconds(60), "as-777");
        BusinessOperation renamed = named.includeObservation(LAST, "as-778");

        assertThat(earlier.firstSeen()).isEqualTo(FIRST.minusSeconds(60));
        assertThat(earlier.lastSeen()).isEqualTo(LAST);
        assertThat(earlier.isProvisional()).isTrue();
        assertThat(named.lastSeen()).isEqualTo(LAST.plusSeconds(60));
        assertThat(named.displayValue()).isEqualTo("as-777");
        assertThat(renamed.displayValue()).isEqualTo("as-777");
    }

    @Test
    @DisplayName("an operation records its trace count and the operation that absorbed it")
    void given_operation_when_countedAndMerged_then_fieldsSet() {
        BusinessOperation operation = anOperation().id(ids.get()).build();
        UUID survivor = ids.get();

        assertThat(operation.withTraceCount(3).traceCount()).isEqualTo(3);
        assertThat(operation.markMergedInto(survivor).mergedInto()).isEqualTo(survivor);
    }

    @Test
    @DisplayName("rejects merging into itself, a reversed interval, a negative count and an id that is not a UUIDv7")
    void given_inconsistentOperation_when_constructedOrMerged_then_invalidBusinessOperation() {
        UUID id = ids.get();
        UUID version4 = UUID.fromString("123e4567-e89b-42d3-a456-426614174000");

        assertViolation(
                ViolationCode.INVALID_BUSINESS_OPERATION,
                () -> anOperation().id(id).build().markMergedInto(id));
        assertViolation(
                ViolationCode.INVALID_BUSINESS_OPERATION,
                () -> new BusinessOperation(id, FIRST, FIRST, 1, "as-777", 1, id));
        assertViolation(
                ViolationCode.INVALID_BUSINESS_OPERATION,
                () -> anOperation().seen(LAST, FIRST).build());
        assertViolation(
                ViolationCode.INVALID_BUSINESS_OPERATION,
                () -> anOperation().traceCount(-1).build());
        assertViolation(
                ViolationCode.INVALID_BUSINESS_OPERATION,
                () -> anOperation().id(version4).build());
    }

    @Test
    @DisplayName("an operation has an id and both seen times")
    void given_missingIdOrTime_when_constructed_then_invalidBusinessOperation() {
        UUID id = ids.get();

        assertViolation(
                ViolationCode.INVALID_BUSINESS_OPERATION,
                () -> new BusinessOperation(null, FIRST, LAST, 1, "x", 1, null));
        assertViolation(
                ViolationCode.INVALID_BUSINESS_OPERATION, () -> new BusinessOperation(id, null, LAST, 1, "x", 1, null));
        assertViolation(
                ViolationCode.INVALID_BUSINESS_OPERATION,
                () -> new BusinessOperation(id, FIRST, null, 1, "x", 1, null));
    }
}
