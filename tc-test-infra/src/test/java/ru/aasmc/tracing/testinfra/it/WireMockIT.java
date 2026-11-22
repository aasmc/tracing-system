package ru.aasmc.tracing.testinfra.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoders;
import org.springframework.security.oauth2.jwt.JwtValidationException;
import ru.aasmc.tracing.testinfra.TcWireMockConfiguration;
import ru.aasmc.tracing.testinfra.TestIds;
import ru.aasmc.tracing.testinfra.TestJwtIssuer;
import ru.aasmc.tracing.testinfra.VaultStubs;

@InfraIT
class WireMockIT {

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    @Autowired
    TestJwtIssuer issuer;

    @Autowired
    VaultStubs vault;

    @Value("${" + TcWireMockConfiguration.ISSUER_URI_PROPERTY + "}")
    String issuerUriProperty;

    @Value("${" + TcWireMockConfiguration.VAULT_URI_PROPERTY + "}")
    String vaultUriProperty;

    @Test
    @DisplayName("the OIDC discovery document and the JWKS are served and registered as properties")
    void given_issuer_when_fetchingDiscoveryAndJwks_then_served() throws Exception {
        HttpResponse<String> discovery = get(issuer.issuerUri() + "/.well-known/openid-configuration");
        HttpResponse<String> jwks = get(issuer.jwkSetUri());

        assertThat(discovery.statusCode()).isEqualTo(200);
        assertThat(discovery.body()).contains("\"issuer\": \"" + issuer.issuerUri() + "\"");
        assertThat(jwks.statusCode()).isEqualTo(200);
        assertThat(jwks.body()).contains("\"kid\":\"tc-test-key\"").doesNotContain("\"d\":");
        assertThat(issuerUriProperty).isEqualTo(issuer.issuerUri());
    }

    @Test
    @DisplayName("a JWT minted by TestJwtIssuer validates with Spring's Nimbus decoder built from the issuer")
    void given_mintedToken_when_decodedViaIssuerLocation_then_validWithRolesAndScopes() {
        JwtDecoder decoder = JwtDecoders.fromIssuerLocation(issuer.issuerUri());

        Jwt jwt = decoder.decode(issuer.piiViewer());

        assertThat(jwt.getIssuer()).hasToString(issuer.issuerUri());
        assertThat(jwt.getClaimAsString("scope")).isEqualTo("openid TC_PII");
        assertThat(jwt.getClaimAsMap("realm_access")).containsEntry("roles", List.of("TC_VIEWER"));
    }

    @Test
    @DisplayName("expired, wrong-issuer and unknown-key tokens are rejected")
    void given_invalidTokens_when_decoded_then_rejected() {
        JwtDecoder decoder = JwtDecoders.fromIssuerLocation(issuer.issuerUri());

        assertThatThrownBy(() -> decoder.decode(
                        issuer.token("u").roles("TC_VIEWER").expired().mint()))
                .isInstanceOf(JwtValidationException.class)
                .hasMessageContaining("expired");
        assertThatThrownBy(() -> decoder.decode(
                        issuer.token("u").roles("TC_VIEWER").wrongIssuer().mint()))
                .isInstanceOf(JwtValidationException.class)
                .hasMessageContaining("iss");
        assertThatThrownBy(() -> decoder.decode(issuer.token("u").unknownKey().mint()))
                .isInstanceOf(BadJwtException.class);
    }

    @Test
    @DisplayName("Vault KV v2 stubs answer 200 with the KV v2 envelope, 403 and 404")
    void given_vaultStubs_when_reading_then_kvV2Responses() throws Exception {
        String ok = TestIds.name("kafka/sms-cluster");
        String denied = TestIds.name("kafka/denied");
        String missing = TestIds.name("kafka/missing");
        vault.secret("secret", ok, Map.of("password", "p\"w"));
        vault.forbidden("secret", denied);
        vault.notFound("secret", missing);

        HttpResponse<String> okResponse = get(vaultUriProperty + VaultStubs.dataPath("secret", ok));

        assertThat(okResponse.statusCode()).isEqualTo(200);
        assertThat(okResponse.body()).contains("\"data\":{\"data\":{\"password\":\"p\\\"w\"}");
        assertThat(get(vaultUriProperty + VaultStubs.dataPath("secret", denied)).statusCode())
                .isEqualTo(403);
        assertThat(get(vaultUriProperty + VaultStubs.dataPath("secret", missing))
                        .statusCode())
                .isEqualTo(404);
    }

    private static HttpResponse<String> get(String uri) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(URI.create(uri)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }
}
