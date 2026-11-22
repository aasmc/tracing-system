package ru.aasmc.tracing.collector.persistence;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import ru.aasmc.tracing.testinfra.TcIntegrationTest;

/**
 * The module variant of {@link TcIntegrationTest}: every test class of {@code tc-persistence} uses it, so the
 * context of {@link PersistenceTestApplication} starts once per test run.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@TcIntegrationTest
public @interface PersistenceIT {}
