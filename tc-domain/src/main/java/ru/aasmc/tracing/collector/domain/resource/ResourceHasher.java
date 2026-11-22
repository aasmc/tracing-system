package ru.aasmc.tracing.collector.domain.resource;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.Map;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeValue;
import ru.aasmc.tracing.collector.domain.attribute.Attributes;

/**
 * Computes ids of resources (the process that produced spans) and instrumentation scopes from their content. Each
 * service instance sends the same resource with every span, so a content id lets storage keep one row per distinct
 * resource instead of one copy per span.
 *
 * <p>The id is the xxHash64 (seed 0) of this byte encoding, in which every text is written as
 * {@code <UTF-8 byte length in decimal> ':' <UTF-8 bytes>}:
 *
 * <ol>
 *   <li>resource: the text {@code schemaUrl}; scope: the texts {@code name}, {@code version}, {@code schemaUrl}
 *       ({@code ""} for {@code null});
 *   <li>then the attributes sorted by key ({@link String#compareTo}), each as the text {@code key}, the text
 *       {@code type} (the {@link ru.aasmc.tracing.collector.domain.attribute.AttributeType} name, e.g.
 *       {@code STRING}), the number of the value's index tokens in decimal followed by {@code ':'}, and each token as
 *       a text.
 * </ol>
 *
 * <p>Example: the resource with schema URL {@code s} and the attribute {@code host.name = "n1"} is encoded as
 * {@code 1:s9:host.name6:STRING1:2:n1}. Because every text carries its length and every array its size, two different
 * contents never have the same encoding, whatever characters they contain; e.g. the arrays {@code []} and
 * {@code [""]} encode as {@code 0:} and {@code 1:0:}. The result does not depend on attribute order or on the JVM.
 * Changing the encoding changes every id.
 */
public final class ResourceHasher {

    private static final char LENGTH_END = ':';

    private ResourceHasher() {}

    public static ResourceId resourceId(String schemaUrl, Attributes attributes) {
        return new ResourceId(hash(attributes, schemaUrl));
    }

    public static InstrumentationScopeId instrumentationScopeId(
            String name, String version, String schemaUrl, Attributes attributes) {
        return new InstrumentationScopeId(hash(attributes, name, version, schemaUrl));
    }

    /** Hashes the header texts (each {@code null} as {@code ""}), then the attributes sorted by key. */
    private static long hash(Attributes attributes, String... headerTexts) {
        ByteArrayOutputStream encoding = new ByteArrayOutputStream();
        for (String text : headerTexts) {
            writeText(encoding, text == null ? "" : text);
        }
        attributes.entries().entrySet().stream()
                .sorted(Map.Entry.comparingByKey(Comparator.comparing(AttributeKey::value)))
                .forEach(attribute -> {
                    AttributeValue value = attribute.getValue();
                    writeText(encoding, attribute.getKey().value());
                    writeText(encoding, value.type().name());
                    writeNumber(encoding, value.indexValues().size());
                    value.indexValues().forEach(token -> writeText(encoding, token));
                });
        return XxHash64.hash(encoding.toByteArray());
    }

    private static void writeText(ByteArrayOutputStream encoding, String text) {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        writeNumber(encoding, bytes.length);
        encoding.writeBytes(bytes);
    }

    private static void writeNumber(ByteArrayOutputStream encoding, int number) {
        encoding.writeBytes((Integer.toString(number) + LENGTH_END).getBytes(StandardCharsets.US_ASCII));
    }
}
