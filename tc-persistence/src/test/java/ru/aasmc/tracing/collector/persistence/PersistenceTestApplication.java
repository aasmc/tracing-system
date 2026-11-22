package ru.aasmc.tracing.collector.persistence;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * {@code tc-persistence} is a library without an application of its own. This one gives its tests what a service
 * gives the repositories: Spring Boot's JDBC auto-configuration and the repository beans of this package.
 */
@SpringBootApplication
public class PersistenceTestApplication {}
