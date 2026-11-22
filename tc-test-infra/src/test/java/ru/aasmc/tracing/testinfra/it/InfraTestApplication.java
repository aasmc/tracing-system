package ru.aasmc.tracing.testinfra.it;

import java.time.Clock;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Minimal application for the Phase-0 tests of the test infrastructure itself (09 §1.4). It has JDBC, Kafka and
 * Web MVC on the classpath, like the services, and one endpoint that reads the {@code Clock} bean.
 */
@SpringBootApplication
public class InfraTestApplication {

    @RestController
    static class ClockController {

        private final Clock clock;

        ClockController(Clock clock) {
            this.clock = clock;
        }

        @GetMapping("/spike/clock")
        String now() {
            return clock.instant().toString();
        }
    }
}
