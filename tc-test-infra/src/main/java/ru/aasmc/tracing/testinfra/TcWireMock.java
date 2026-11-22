package ru.aasmc.tracing.testinfra;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;

import com.github.tomakehurst.wiremock.WireMockServer;

/**
 * JVM-wide WireMock server for the external HTTP endpoints of TCS: the OIDC issuer (discovery document
 * and JWKS, {@link TestJwtIssuer}) and the secret store (Vault KV v2, {@link VaultStubs}). Nothing else is mocked.
 *
 * <p>Uses the shaded {@code wiremock-standalone} artifact, so WireMock's Jetty and Jackson 2 cannot clash with the
 * Jetty 12 / Jackson 3 versions managed by Spring Boot 4.
 */
public final class TcWireMock {

    private TcWireMock() {}

    public static WireMockServer server() {
        return Holder.INSTANCE;
    }

    public static String baseUrl() {
        return server().baseUrl();
    }

    private static final class Holder {
        static final WireMockServer INSTANCE = start();

        private static WireMockServer start() {
            WireMockServer server = new WireMockServer(options().dynamicPort());
            server.start();
            Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "tc-wiremock-stop"));
            return server;
        }
    }
}
