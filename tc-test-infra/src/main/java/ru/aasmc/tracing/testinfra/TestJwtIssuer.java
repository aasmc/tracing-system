package ru.aasmc.tracing.testinfra;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * A fake OIDC issuer on {@link TcWireMock}. Publishes the discovery document and the JWKS, and mints
 * RS256 JWTs carrying the TCS authorities: roles in {@code realm_access.roles}
 * ({@code TC_VIEWER}, {@code TC_ADMIN}) and the scope {@code TC_PII}.
 *
 * <p>Tokens are timestamped with the system time, not the test clock, because Spring Security validates
 * {@code exp}/{@code nbf} against its own clock.
 */
public final class TestJwtIssuer {

    public static final String REALM_PATH = "/realms/tcs";
    public static final String JWKS_PATH = REALM_PATH + "/protocol/openid-connect/certs";
    public static final String DISCOVERY_PATH = REALM_PATH + "/.well-known/openid-configuration";
    public static final String ROLE_VIEWER = "TC_VIEWER";
    public static final String ROLE_ADMIN = "TC_ADMIN";
    public static final String SCOPE_PII = "TC_PII";

    private final RSAKey signingKey;
    private final RSAKey foreignKey;
    private final String issuer;

    private TestJwtIssuer(WireMockServer server) {
        this.signingKey = generateKey("tc-test-key");
        this.foreignKey = generateKey("unknown-key");
        this.issuer = server.baseUrl() + REALM_PATH;
        publish(server);
    }

    /** The JVM-wide issuer; stubs are (re)published on first access. */
    public static TestJwtIssuer instance() {
        return Holder.INSTANCE;
    }

    public String issuerUri() {
        return issuer;
    }

    public String jwkSetUri() {
        return TcWireMock.baseUrl() + JWKS_PATH;
    }

    /** Starts a token for {@code subject}, valid for 5 minutes, issued by this issuer, without roles or scopes. */
    public TokenBuilder token(String subject) {
        return new TokenBuilder(subject);
    }

    /** A {@code TC_VIEWER} token. */
    public String viewer() {
        return token("viewer").roles(ROLE_VIEWER).mint();
    }

    /** A {@code TC_VIEWER} token with the {@code TC_PII} scope. */
    public String piiViewer() {
        return token("pii-viewer").roles(ROLE_VIEWER).scopes(SCOPE_PII).mint();
    }

    /** A {@code TC_ADMIN} token. */
    public String admin() {
        return token("admin").roles(ROLE_ADMIN).mint();
    }

    private void publish(WireMockServer server) {
        String jwks = new JWKSet(signingKey.toPublicJWK()).toString();
        server.stubFor(get(urlPathEqualTo(JWKS_PATH)).willReturn(okJson(jwks)));
        String discovery = """
                {
                  "issuer": "%s",
                  "jwks_uri": "%s",
                  "authorization_endpoint": "%s/protocol/openid-connect/auth",
                  "token_endpoint": "%s/protocol/openid-connect/token",
                  "response_types_supported": ["code"],
                  "subject_types_supported": ["public"],
                  "id_token_signing_alg_values_supported": ["RS256"]
                }
                """.formatted(issuer, server.baseUrl() + JWKS_PATH, issuer, issuer);
        server.stubFor(get(urlPathEqualTo(DISCOVERY_PATH)).willReturn(okJson(discovery)));
    }

    private static RSAKey generateKey(String keyId) {
        try {
            return new RSAKeyGenerator(2048).keyID(keyId).generate();
        } catch (JOSEException e) {
            throw new IllegalStateException("Cannot generate RSA key", e);
        }
    }

    /** Fluent JWT builder; every method returns this builder. */
    public final class TokenBuilder {

        private final String subject;
        private final List<String> roles = new ArrayList<>();
        private final List<String> scopes = new ArrayList<>();
        private Instant issuedAt = Instant.now();
        private Duration lifetime = Duration.ofMinutes(5);
        private String tokenIssuer = issuer;
        private RSAKey key = signingKey;

        private TokenBuilder(String subject) {
            this.subject = Objects.requireNonNull(subject, "subject");
        }

        public TokenBuilder roles(String... values) {
            roles.addAll(List.of(values));
            return this;
        }

        public TokenBuilder scopes(String... values) {
            scopes.addAll(List.of(values));
            return this;
        }

        public TokenBuilder lifetime(Duration value) {
            this.lifetime = Objects.requireNonNull(value, "value");
            return this;
        }

        /** Issued 1 hour ago, expired 55 minutes ago. */
        public TokenBuilder expired() {
            this.issuedAt = Instant.now().minus(Duration.ofHours(1));
            this.lifetime = Duration.ofMinutes(5);
            return this;
        }

        /** Signed with the published key but carrying another {@code iss}. */
        public TokenBuilder wrongIssuer() {
            this.tokenIssuer = "https://other-idp.example/realms/other";
            return this;
        }

        /** Signed with a key that is not in the JWKS. */
        public TokenBuilder unknownKey() {
            this.key = foreignKey;
            return this;
        }

        public String mint() {
            List<String> allScopes = new ArrayList<>();
            allScopes.add("openid");
            allScopes.addAll(scopes);
            JWTClaimsSet claims = new JWTClaimsSet.Builder()
                    .issuer(tokenIssuer)
                    .subject(subject)
                    .audience("tcs-api")
                    .jwtID(UUID.randomUUID().toString())
                    .issueTime(Date.from(issuedAt))
                    .notBeforeTime(Date.from(issuedAt))
                    .expirationTime(Date.from(issuedAt.plus(lifetime)))
                    .claim("preferred_username", subject)
                    .claim("scope", String.join(" ", allScopes))
                    .claim("realm_access", Map.of("roles", List.copyOf(roles)))
                    .build();
            JWSHeader header = new JWSHeader.Builder(JWSAlgorithm.RS256)
                    .keyID(key.getKeyID())
                    .type(JOSEObjectType.JWT)
                    .build();
            SignedJWT jwt = new SignedJWT(header, claims);
            try {
                jwt.sign(new RSASSASigner(key));
            } catch (JOSEException e) {
                throw new IllegalStateException("Cannot sign JWT", e);
            }
            return jwt.serialize();
        }
    }

    private static final class Holder {
        static final TestJwtIssuer INSTANCE = new TestJwtIssuer(TcWireMock.server());
    }
}
