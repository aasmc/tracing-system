package ru.aasmc.tracing.collector.domain.resource;

import static org.assertj.core.api.Assertions.assertThat;
import static ru.aasmc.tracing.collector.testmodel.domain.AttributesBuilder.attributes;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue.StringArrayValue;
import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/** Expected ids come from an independent xxHash64 implementation applied to the documented encoding. */
class ResourceHasherTest {

    private static final String SCHEMA_URL = "https://opentelemetry.io/schemas/1.26.0";

    private static final Attributes RESOURCE = attributes()
            .put("service.name", "idp-gateway")
            .put("host.name", "node-1")
            .put("process.pid", 4242)
            .build();

    private static final ResourceId RESOURCE_ID = new ResourceId(-6094372945853116807L);

    @Test
    @DisplayName("a resource id is the xxHash64 of its encoding")
    void given_resource_when_hashed_then_goldenValue() {
        assertThat(ResourceHasher.resourceId(SCHEMA_URL, RESOURCE)).isEqualTo(RESOURCE_ID);
    }

    @Test
    @DisplayName("an instrumentation scope id is the xxHash64 of its encoding, with a missing schema URL as empty")
    void given_scope_when_hashed_then_goldenValue() {
        Attributes scopeAttributes = attributes()
                .put("scope.kind", new StringArrayValue(List.of("a", "b"), false))
                .build();

        assertThat(ResourceHasher.instrumentationScopeId(
                        "io.opentelemetry.spring-webmvc", "2.10.0", null, scopeAttributes))
                .isEqualTo(new InstrumentationScopeId(-5260085291935310900L));
    }

    @Test
    @DisplayName("the id does not depend on attribute order")
    void given_attributesInOtherOrder_when_hashed_then_sameId() {
        Attributes reordered = attributes()
                .put("process.pid", 4242)
                .put("service.name", "idp-gateway")
                .put("host.name", "node-1")
                .build();

        assertThat(ResourceHasher.resourceId(SCHEMA_URL, reordered)).isEqualTo(RESOURCE_ID);
    }

    static Stream<Arguments> contentsThatLookAlike() {
        return Stream.of(
                Arguments.of(
                        "an empty array and an array of one empty string",
                        attributes()
                                .put("tags", new StringArrayValue(List.of(), false))
                                .build(),
                        attributes()
                                .put("tags", new StringArrayValue(List.of(""), false))
                                .build()),
                Arguments.of(
                        "one element holding a separator character and two elements",
                        attributes()
                                .put("tags", new StringArrayValue(List.of("a\u0002b"), false))
                                .build(),
                        attributes()
                                .put("tags", new StringArrayValue(List.of("a", "b"), false))
                                .build()),
                Arguments.of(
                        "a value holding a line break and the next attribute, and that attribute itself",
                        attributes().put("a", "x\nb\u0001STRING\u0001y").build(),
                        attributes().put("a", "x").put("b", "y").build()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("contentsThatLookAlike")
    @DisplayName("""
            Given two different contents that a separator-based encoding would write the same way,
            when they are hashed,
            then their ids differ: every text carries its length and every array its size""")
    void given_contentsThatLookAlike_when_hashed_then_differentIds(String name, Attributes one, Attributes other) {
        assertThat(ResourceHasher.resourceId(SCHEMA_URL, one))
                .isNotEqualTo(ResourceHasher.resourceId(SCHEMA_URL, other));
    }

    @Test
    @DisplayName("a schema URL holding a line break does not imitate an attribute")
    void given_schemaUrlWithLineBreak_when_hashed_then_differsFromAttribute() {
        assertThat(ResourceHasher.resourceId(
                        "s\nk\u0001STRING\u0001v", attributes().build()))
                .isNotEqualTo(ResourceHasher.resourceId(
                        "s", attributes().put("k", "v").build()));
    }

    static Stream<Arguments> changedResources() {
        return Stream.of(
                Arguments.of("other schema URL", "https://opentelemetry.io/schemas/1.27.0", RESOURCE),
                Arguments.of(
                        "other value",
                        SCHEMA_URL,
                        attributes()
                                .put("service.name", "idp-gateway")
                                .put("host.name", "node-2")
                                .put("process.pid", 4242)
                                .build()),
                Arguments.of(
                        "other key",
                        SCHEMA_URL,
                        attributes()
                                .put("service.name", "idp-gateway")
                                .put("host.id", "node-1")
                                .put("process.pid", 4242)
                                .build()),
                Arguments.of(
                        "same text, other type",
                        SCHEMA_URL,
                        attributes()
                                .put("service.name", "idp-gateway")
                                .put("host.name", "node-1")
                                .put("process.pid", "4242")
                                .build()),
                Arguments.of(
                        "missing attribute",
                        SCHEMA_URL,
                        attributes()
                                .put("service.name", "idp-gateway")
                                .put("host.name", "node-1")
                                .build()),
                Arguments.of(
                        "extra attribute",
                        SCHEMA_URL,
                        attributes()
                                .put("service.name", "idp-gateway")
                                .put("host.name", "node-1")
                                .put("process.pid", 4242)
                                .put("host.arch", "amd64")
                                .build()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("changedResources")
    @DisplayName("any change of the content changes the id")
    void given_changedResource_when_hashed_then_otherId(String change, String schemaUrl, Attributes attributes) {
        assertThat(ResourceHasher.resourceId(schemaUrl, attributes)).isNotEqualTo(RESOURCE_ID);
    }
}
