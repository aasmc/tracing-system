package ru.aasmc.tracing.testinfra.it;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import ru.aasmc.tracing.testinfra.TcIntegrationTest;

/** The module variant of {@link TcIntegrationTest}: every test class of this module uses it (09 §0.3 T6). */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@TcIntegrationTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@AutoConfigureRestTestClient
public @interface InfraIT {}
