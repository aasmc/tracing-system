package ru.aasmc.tracing.testinfra;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;

import com.github.tomakehurst.wiremock.WireMockServer;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * WireMock stubs for the Vault KV v2 HTTP API (09 §1.3): {@code GET /v1/<mount>/data/<path>}. Each test uses its own
 * unique path ({@link TestIds}), so stubs of different tests never collide.
 */
public final class VaultStubs {

    /** Vault token the stubs accept; any value works, it is not checked. */
    public static final String TOKEN = "tc-test-vault-token";

    private final WireMockServer server;

    public VaultStubs(WireMockServer server) {
        this.server = server;
    }

    public static VaultStubs onTcWireMock() {
        return new VaultStubs(TcWireMock.server());
    }

    public String vaultUri() {
        return server.baseUrl();
    }

    /** 200 with the KV v2 envelope {@code {"data":{"data":{...},"metadata":{...}}}}. */
    public void secret(String mount, String path, Map<String, String> data) {
        server.stubFor(get(urlPathEqualTo(dataPath(mount, path))).willReturn(okJson(secretBody(data))));
    }

    private static String secretBody(Map<String, String> data) {
        String json = data.entrySet().stream()
                .map(e -> quote(e.getKey()) + ":" + quote(e.getValue()))
                .collect(Collectors.joining(",", "{", "}"));
        return """
                {"request_id":"%s","lease_id":"","renewable":false,"lease_duration":0,
                 "data":{"data":%s,
                         "metadata":{"created_time":"2026-11-22T00:00:00Z","custom_metadata":null,
                                     "deletion_time":"","destroyed":false,"version":1}},
                 "wrap_info":null,"warnings":null,"auth":null}
                """.formatted(UUID.randomUUID(), json);
    }

    /** 403 {@code permission denied}. */
    public void forbidden(String mount, String path) {
        server.stubFor(get(urlPathEqualTo(dataPath(mount, path)))
                .willReturn(aResponse()
                        .withStatus(403)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"errors\":[\"1 error occurred:\\n\\t* permission denied\\n\\n\"]}")));
    }

    /** 404 with an empty error list, as Vault answers for a missing secret. */
    public void notFound(String mount, String path) {
        server.stubFor(get(urlPathEqualTo(dataPath(mount, path)))
                .willReturn(aResponse()
                        .withStatus(404)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"errors\":[]}")));
    }

    /** A successful answer delayed by {@code delay}, to exercise client timeouts. */
    public void slow(String mount, String path, Map<String, String> data, Duration delay) {
        server.stubFor(get(urlPathEqualTo(dataPath(mount, path)))
                .willReturn(okJson(secretBody(data)).withFixedDelay(Math.toIntExact(delay.toMillis()))));
    }

    public static String dataPath(String mount, String path) {
        return "/v1/" + mount + "/data/" + path;
    }

    private static String quote(String value) {
        StringBuilder sb = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.append('"').toString();
    }
}
