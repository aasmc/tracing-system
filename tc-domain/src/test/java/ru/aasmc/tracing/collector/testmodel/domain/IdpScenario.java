package ru.aasmc.tracing.collector.testmodel.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import ru.aasmc.tracing.collector.domain.attribute.AttributeKey;
import ru.aasmc.tracing.collector.domain.attribute.AttributeType;
import ru.aasmc.tracing.collector.domain.correlation.CorrelationKey;
import ru.aasmc.tracing.collector.domain.correlation.CorrelationObservation;
import ru.aasmc.tracing.collector.domain.correlation.CorrelationType;
import ru.aasmc.tracing.collector.domain.correlation.KeyRole;
import ru.aasmc.tracing.collector.domain.correlation.MemberTrace;
import ru.aasmc.tracing.collector.domain.correlation.OutcomeRule;
import ru.aasmc.tracing.collector.domain.enrichment.EnrichmentScope;
import ru.aasmc.tracing.collector.domain.enrichment.EnrichmentSourceDefinition;
import ru.aasmc.tracing.collector.domain.enrichment.FieldMapping;
import ru.aasmc.tracing.collector.domain.enrichment.FieldSource;
import ru.aasmc.tracing.collector.domain.enrichment.JsonPointer;
import ru.aasmc.tracing.collector.domain.enrichment.TraceContextSource;
import ru.aasmc.tracing.collector.domain.enrichment.Transform;
import ru.aasmc.tracing.collector.domain.trace.Span;
import ru.aasmc.tracing.collector.domain.trace.SpanKind;
import ru.aasmc.tracing.collector.domain.trace.TraceId;

/**
 * A user logs in to {@code bookstore-web} through an identity provider in three requests, each its own trace:
 *
 * <ol>
 *   <li>T1, first factor: password check, session {@code as-777} starts;
 *   <li>T2, second factor: SMS code check in the same session, which issues authorization code {@code code-abc};
 *   <li>T3, token exchange: the client redeems {@code code-abc}.
 * </ol>
 *
 * The correlation type {@code idp.login} links them into one business operation: T1–T2 share the session id
 * (primary key), T2–T3 share the authorization code (alias key). An SMS gateway reports the delivery of the SMS with
 * a {@code traceparent} pointing at the T2 span that sent it.
 */
public final class IdpScenario {

    public static final String GATEWAY = "idp-gateway";
    public static final String AUTH = "idp-auth-service";
    public static final String OTP = "idp-otp-service";
    public static final String TOKEN = "idp-token-service";

    /** Dictionary ids of the services. */
    public static final Map<String, Integer> SERVICE_IDS = Map.of(GATEWAY, 1, AUTH, 2, OTP, 3, TOKEN, 4);

    /** Dictionary ids of the root span names. */
    public static final Map<String, Integer> NAME_IDS =
            Map.of("POST /login/first-factor", 1, "POST /login/second-factor", 2, "POST /oauth2/token", 3);

    public static final TraceId T1 = DomainIds.traceId("4bf92f3577b34da6a3ce929d0e0e4736");
    public static final TraceId T2 = DomainIds.traceId("7a3c1e9d2b4f60851c2d3e4f5a6b7c8d");
    public static final TraceId T3 = DomainIds.traceId("c0ffee00d15ea5e0123456789abcdef0");

    public static final String SESSION = "as-777";
    public static final String CODE = "code-abc";
    public static final String STEP_AUTHENTICATION = "authentication";
    public static final String STEP_TOKEN_EXCHANGE = "token-exchange";
    public static final int SESSION_KEY_ID = 1;
    public static final int CODE_KEY_ID = 2;

    /** When each trace's correlation values were seen. */
    public static final Instant T1_OBSERVED = Instant.parse("2026-11-21T09:15:02.130Z");

    public static final Instant T2_OBSERVED = Instant.parse("2026-11-21T09:15:31.600Z");
    public static final Instant T3_OBSERVED = Instant.parse("2026-11-21T09:15:33.044Z");

    /** The SMS delivery report's header: T2, span {@code send-sms-otp}, sampled. */
    public static final String SMS_TRACEPARENT = "00-7a3c1e9d2b4f60851c2d3e4f5a6b7c8d-5f3a9b2c1d0e4f68-01";

    private IdpScenario() {}

    /** The spans of a trace, root first. */
    public static List<Span> spans(TraceId trace) {
        if (trace.equals(T1)) {
            Span root = rootSpan(T1, "a1b2c3d4e5f60718", "POST /login/first-factor", "2026-11-21T09:15:02.114Z", 182);
            return List.of(
                    root,
                    childSpan(root, "b1c2d3e4f5061728", AUTH, "authenticate-password", T1_OBSERVED)
                            .attribute("user_id", "u-42")
                            .attribute("client_id", "bookstore-web")
                            .attribute("auth.factor", "PASSWORD")
                            .attribute("idp.auth_session_id", SESSION)
                            .build());
        }
        if (trace.equals(T2)) {
            Span root = rootSpan(T2, "1a2b3c4d5e6f7081", "POST /login/second-factor", "2026-11-21T09:15:31.502Z", 409);
            Span auth = childSpan(root, "9c8b7a6d5e4f3021", AUTH, "verify-second-factor", T2_OBSERVED)
                    .attribute("auth.factor", "SMS")
                    .attribute("idp.auth_session_id", SESSION)
                    .attribute("oauth.authorization_code", CODE)
                    .build();
            Span sms = childSpan(
                            auth, "5f3a9b2c1d0e4f68", OTP, "send-sms-otp", Instant.parse("2026-11-21T09:15:31.640Z"))
                    .kind(SpanKind.CLIENT)
                    .build();
            return List.of(root, auth, sms);
        }
        if (trace.equals(T3)) {
            Span root = rootSpan(T3, "c3d4e5f607182930", "POST /oauth2/token", "2026-11-21T09:15:33.020Z", 96);
            return List.of(
                    root,
                    childSpan(root, "d4e5f60718293a4b", TOKEN, "exchange-code", T3_OBSERVED)
                            .attribute("oauth.authorization_code", CODE)
                            .build());
        }
        throw new IllegalArgumentException("not an IdP trace");
    }

    /** {@code idp.login}: primary key session id, alias key authorization code; complete once tokens are issued. */
    public static CorrelationType correlationType() {
        return new CorrelationType(
                1,
                "idp.login",
                Duration.ofDays(1),
                List.of(
                        new CorrelationKey(
                                SESSION_KEY_ID,
                                new AttributeKey("idp.auth_session_id"),
                                KeyRole.PRIMARY,
                                STEP_AUTHENTICATION),
                        new CorrelationKey(
                                CODE_KEY_ID,
                                new AttributeKey("oauth.authorization_code"),
                                KeyRole.ALIAS,
                                STEP_TOKEN_EXCHANGE)),
                new OutcomeRule(null, Set.of(), Set.of(), STEP_TOKEN_EXCHANGE));
    }

    /** The correlation values each trace carries. */
    public static List<CorrelationObservation> observations(TraceId trace) {
        if (trace.equals(T1)) {
            return List.of(observation(T1, T1_OBSERVED, SESSION_KEY_ID, SESSION));
        }
        if (trace.equals(T2)) {
            return List.of(
                    observation(T2, T2_OBSERVED, SESSION_KEY_ID, SESSION),
                    observation(T2, T2_OBSERVED, CODE_KEY_ID, CODE));
        }
        if (trace.equals(T3)) {
            return List.of(observation(T3, T3_OBSERVED, CODE_KEY_ID, CODE));
        }
        throw new IllegalArgumentException("not an IdP trace");
    }

    public static CorrelationObservation observation(TraceId trace, Instant seen, int keyId, String value) {
        return new CorrelationObservation(seen, 1, keyId, value, trace);
    }

    /** The three traces as outcome evaluation sees them: no errors, labelled by the keys that linked them. */
    public static List<MemberTrace> memberTraces() {
        return List.of(
                new MemberTrace(T1, T1_OBSERVED, 0, null, Set.of(STEP_AUTHENTICATION)),
                new MemberTrace(T2, T2_OBSERVED, 0, null, Set.of(STEP_AUTHENTICATION, STEP_TOKEN_EXCHANGE)),
                new MemberTrace(T3, T3_OBSERVED, 0, null, Set.of(STEP_TOKEN_EXCHANGE)));
    }

    /** The SMS gateway's delivery reports: span scope via {@code traceparent}; status and provider become attributes. */
    public static EnrichmentSourceDefinition smsDefinition() {
        return new EnrichmentSourceDefinition(
                new TraceContextSource.TraceparentHeader(null),
                EnrichmentScope.SPAN,
                List.of(payloadMapping("/status", "sms.delivery.status"), payloadMapping("/provider", "sms.provider")));
    }

    /** A PAYLOAD mapping to a STRING attribute without transform. */
    public static FieldMapping payloadMapping(String pointer, String targetKey) {
        return new FieldMapping(
                FieldSource.PAYLOAD,
                new JsonPointer(pointer),
                null,
                new AttributeKey(targetKey),
                AttributeType.STRING,
                new Transform.None(),
                false);
    }

    private static Span rootSpan(TraceId trace, String spanId, String name, String start, long millis) {
        return SpanBuilder.aSpan()
                .traceId(trace)
                .spanId(spanId)
                .service(GATEWAY)
                .name(name)
                .start(Instant.parse(start))
                .duration(Duration.ofMillis(millis))
                .build();
    }

    private static SpanBuilder childSpan(Span parent, String spanId, String service, String name, Instant start) {
        return SpanBuilder.aChildOf(parent, spanId)
                .service(service)
                .name(name)
                .kind(SpanKind.SERVER)
                .start(start)
                .duration(Duration.ofMillis(50));
    }
}
