package ru.aasmc.tracing.testinfra;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.core.annotation.AliasFor;
import org.springframework.test.context.ActiveProfiles;

/**
 * The only way to write a TCS integration test: the full Spring Boot context of the module, the
 * JVM-wide containers, WireMock and the mutable clock, profile {@code it}.
 *
 * <p>Each module defines its own variant that adds module-specific imports, e.g.
 *
 * <pre>{@code
 * @TcIntegrationTest
 * @Import(IngesterTestConfiguration.class)
 * public @interface IngesterIT {}
 * }</pre>
 *
 * All test classes of a module use that one variant, so Spring's context cache starts the context once per JVM.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@SpringBootTest
@Import({TcContainersConfiguration.class, TcWireMockConfiguration.class, ClockTestConfiguration.class})
@ActiveProfiles("it")
public @interface TcIntegrationTest {

    /** {@code RANDOM_PORT} for the web modules ({@code tc-query-api}, {@code tc-control-plane}). */
    @AliasFor(annotation = SpringBootTest.class, attribute = "webEnvironment")
    SpringBootTest.WebEnvironment webEnvironment() default SpringBootTest.WebEnvironment.MOCK;

    /** Extra properties; prefer the module's {@code application-it.yaml}. */
    @AliasFor(annotation = SpringBootTest.class, attribute = "properties")
    String[] properties() default {};
}
