package ru.aasmc.tracing.collector.domain.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** {@code tc-domain} stays framework-free. Plain JUnit: ArchUnit analyses bytecode, no context needed. */
class DomainArchitectureTest {

    private static final JavaClasses DOMAIN = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("ru.aasmc.tracing.collector.domain");

    @Test
    @DisplayName("domain main code has no Spring, JDBC, Kafka or Jackson dependency")
    void given_domainMainCode_when_analysed_then_noFrameworkDependencies() {
        ArchRule rule = noClasses()
                .that()
                .resideInAPackage("ru.aasmc.tracing.collector.domain..")
                .should()
                .dependOnClassesThat()
                .resideInAnyPackage(
                        "org.springframework..",
                        "java.sql..",
                        "javax.sql..",
                        "org.apache.kafka..",
                        "tools.jackson..",
                        "com.fasterxml.jackson..");

        rule.check(DOMAIN);
    }
}
