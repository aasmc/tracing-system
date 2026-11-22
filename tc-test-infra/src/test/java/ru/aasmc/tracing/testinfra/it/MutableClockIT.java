package ru.aasmc.tracing.testinfra.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.client.RestTestClient;
import ru.aasmc.tracing.testinfra.MutableClock;

/** 0-T4, and spike 0-7: the HTTP client of full-context web tests is Spring Framework 7's {@link RestTestClient}. */
@InfraIT
class MutableClockIT {

    @Autowired
    Clock clock;

    @Autowired
    MutableClock mutableClock;

    @Autowired
    RestTestClient client;

    @AfterEach
    void resetClock() {
        mutableClock.reset();
    }

    @Test
    @DisplayName("0-T4 the context's Clock bean is the mutable test clock, in UTC")
    void given_context_when_injectingClock_then_mutableClock() {
        assertThat(clock).isSameAs(mutableClock);
        assertThat(clock.getZone().getId()).isEqualTo("Z");
    }

    @Test
    @DisplayName("0-T4 setting and advancing the clock changes what production code observes, over real HTTP")
    void given_clockSetAndAdvanced_when_endpointReadsClock_then_seesTestTime() {
        Instant t0 = Instant.parse("2026-11-21T23:59:59Z");
        mutableClock.setInstant(t0);
        mutableClock.advance(Duration.ofSeconds(2));

        client.get()
                .uri("/spike/clock")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody(String.class)
                .isEqualTo("2026-11-22T00:00:01Z");
    }
}
