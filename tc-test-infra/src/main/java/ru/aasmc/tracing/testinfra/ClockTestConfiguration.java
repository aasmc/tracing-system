package ru.aasmc.tracing.testinfra;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Replaces the production {@code Clock} bean with a {@link MutableClock} (09 §0.3 T5, T10). The bean is a
 * {@code java.time.Clock}, so it is injected wherever production code asks for a {@code Clock}.
 */
@TestConfiguration(proxyBeanMethods = false)
public class ClockTestConfiguration {

    @Bean
    @Primary
    MutableClock mutableClock() {
        return MutableClock.startingNow();
    }
}
