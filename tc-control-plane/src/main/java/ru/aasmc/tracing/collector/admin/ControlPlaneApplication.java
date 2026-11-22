package ru.aasmc.tracing.collector.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Entry point of {@code tc-control-plane} (Administration API for sources, correlation, indexing, retention (03 §5)). */
@SpringBootApplication
public class ControlPlaneApplication {

    public static void main(String[] args) {
        SpringApplication.run(ControlPlaneApplication.class, args);
    }
}
