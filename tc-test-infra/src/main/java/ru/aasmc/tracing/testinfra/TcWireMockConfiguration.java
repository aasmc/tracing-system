package ru.aasmc.tracing.testinfra;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;

/**
 * Points the application at the WireMock-backed OIDC issuer and Vault (not named
 * {@code WireMockConfiguration} to avoid a clash with WireMock's own class). Exposes {@link TestJwtIssuer} and
 * {@link VaultStubs} as beans so tests can inject them.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TcWireMockConfiguration {

    public static final String ISSUER_URI_PROPERTY = "spring.security.oauth2.resourceserver.jwt.issuer-uri";
    public static final String JWK_SET_URI_PROPERTY = "spring.security.oauth2.resourceserver.jwt.jwk-set-uri";
    public static final String VAULT_URI_PROPERTY = "tc.secrets.vault.uri";

    @Bean
    TestJwtIssuer testJwtIssuer() {
        return TestJwtIssuer.instance();
    }

    @Bean
    VaultStubs vaultStubs() {
        return VaultStubs.onTcWireMock();
    }

    @Bean
    DynamicPropertyRegistrar wireMockProperties(TestJwtIssuer issuer, VaultStubs vault) {
        return registry -> {
            registry.add(ISSUER_URI_PROPERTY, issuer::issuerUri);
            registry.add(JWK_SET_URI_PROPERTY, issuer::jwkSetUri);
            registry.add(VAULT_URI_PROPERTY, vault::vaultUri);
        };
    }
}
