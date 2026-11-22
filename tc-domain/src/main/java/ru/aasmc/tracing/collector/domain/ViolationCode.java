package ru.aasmc.tracing.collector.domain;

/**
 * Machine-readable codes of domain rule violations (09 §2.1 item 1a-10).
 *
 * <p>The control plane returns them as {@code errors[].code} of a {@code ProblemDetail} (03 §5.4, §5.9); the
 * data-plane services log and count them. Codes of 03 keep their names; codes marked "new" were introduced by the
 * domain API and are listed in 09 §2.6.
 */
public enum ViolationCode {

    // ---- shared-kernel value objects (05 §3) ----
    /** 05 §3.1: not 32 hex characters, or all zeros. */
    INVALID_TRACE_ID,
    /** 05 §3.2: not 16 hex characters, or all zeros. */
    INVALID_SPAN_ID,
    /** New. 05 §3.3: a time range whose upper bound is not after its lower bound. */
    INVALID_TIME_RANGE,
    /** 03 §4 ({@code /problems/window-too-large}): a time range longer than the allowed maximum. */
    WINDOW_TOO_LARGE,
    /** New. 05 §3.4: blank, longer than 255 characters, or containing control characters. */
    INVALID_ATTRIBUTE_KEY,
    /** 05 §3.4: an attribute value that cannot be represented (e.g. NaN, null element, oversized string). */
    INVALID_ATTRIBUTE_TYPE,
    /** New. 05 §3.4: more than {@code Attributes.MAX_ENTRIES} attributes. */
    TOO_MANY_ATTRIBUTES,
    /** New. 05 §3.6: a span violating its invariants (own parent, negative duration, unflagged truncation, ...). */
    INVALID_SPAN,
    /** 05 §3.6 rule 1: a span whose trace id differs from the fragment's trace id. */
    TRACE_ID_MISMATCH,
    /** 05 §3.7: an encoded fragment larger than 8 MiB. */
    FRAGMENT_TOO_LARGE,
    /** New. 05 §4.4: a trace summary violating its invariants, or a merge of summaries of different traces. */
    INVALID_TRACE_SUMMARY,
    /** New. 05 §3.5: a resource or scope whose content-hash id does not match its content. */
    CONTENT_HASH_MISMATCH,
    /** New. 05 §3.7: negative Kafka partition or offset. */
    INVALID_KAFKA_COORDINATES,

    // ---- enrichment source definition: 03 §5.4 V1..V15 ----
    /** V1: name does not match {@code ^[a-z0-9][a-z0-9._-]{2,62}$}. */
    INVALID_NAME,
    /** V2: not a legal Kafka topic name. */
    INVALID_TOPIC,
    /** V3: not an RFC 6901 JSON Pointer. */
    INVALID_JSON_POINTER,
    /** V4: trace-context strategy without its required locator. */
    INVALID_TRACE_CONTEXT,
    /** V5: wrong number of field mappings, or a mapping without the locator its source needs. */
    INVALID_MAPPING,
    /** New (split from V6): a target key that does not match {@code ^[a-z][a-z0-9_.-]{0,127}$}. */
    INVALID_TARGET_KEY,
    /** V6: the same target key used by two mappings of one source. */
    DUPLICATE_TARGET_KEY,
    /** V7: a reserved prefix ({@code tc.}, {@code otel.}, {@code telemetry.}, {@code service.}) or built-in key. */
    RESERVED_KEY,
    /** V8 (checked by the control plane, needs the {@code attribute_key} dictionary): a key has one value type. */
    VALUE_TYPE_CONFLICT,
    /** V9: unknown transform, bad argument, or a string-only transform on a non-STRING mapping. */
    INVALID_TRANSFORM,
    /** V10: SPAN scope with a strategy that does not yield a span id. */
    INVALID_SCOPE,
    /** V11: an unsupported start position, missing-context policy or payload format. */
    INVALID_OPTION,
    /** V12: a filter without a valid pointer or without a scalar value. */
    INVALID_FILTER,
    /** V13: consumer overrides out of bounds. */
    INVALID_CONSUMER_OPTIONS,
    /** V14: inconsistent security protocol, SASL mechanism, or secret reference. */
    INVALID_CONNECTION,
    /** V15: a client property that TCS owns or that is not on the allow-list. */
    FORBIDDEN_CLIENT_PROPERTY,

    // ---- enrichment source lifecycle (05 §4.7.2, 06 UC-05) ----
    /** 06 UC-05: an ACTIVE source must be paused before it is deleted. */
    SOURCE_ACTIVE,
    /** New. 05 §4.7.2: only an ACTIVE source can be paused. */
    SOURCE_NOT_ACTIVE,
    /** New. 05 §4.7.2: a DELETED source is immutable. */
    SOURCE_DELETED,
    /** 05 §4.7.2 invariant 2: topic and connection of a source never change. */
    TOPIC_IMMUTABLE,
    /** New. 05 §4.7.2: revision numbers start at 1. */
    INVALID_REVISION,

    // ---- indexed attributes and digest retention (05 §4.9, 03 §5.9) ----
    /** 03 §5.9: a digest transform outside {NONE, SHA256, MASK}, or a transform on a non-retained key. */
    INVALID_DIGEST_TRANSFORM,
    /** 03 §5.9: creating an indexed attribute for retention without an origin scope. */
    ORIGIN_SCOPE_REQUIRED,
    /** 03 §5.9 ({@code /problems/retained-key-must-be-indexed}): a DISABLED key cannot be retained. */
    RETAINED_KEY_MUST_BE_INDEXED,
    /** 03 §5.9 ({@code /problems/digest-transform-change}): changing the transform of a retained key needs force. */
    DIGEST_TRANSFORM_CHANGE,
    /** 03 §5.9, 05 §7.3: a PERSONAL key retained in clear without {@code acknowledgePii}. */
    PII_RETAINED_IN_CLEAR,
    /** 03 §5.9, 05 §7.3: a SECRET key retained with transform NONE. */
    SECRET_KEY_REQUIRES_TRANSFORM,
    /** New. 05 §4.9: built-in keys cannot be disabled. */
    BUILT_IN_KEY,
    /** New. 05 §4.9: a status change that the indexed-attribute state machine does not allow. */
    INVALID_INDEX_TRANSITION,

    // ---- retention tiers: 03 §5.9 R1..R3 ----
    /** R1: {@code fullDays < 2}. */
    FULL_DAYS_TOO_SMALL,
    /** R2: {@code fullDays} not greater than the largest merge window in days (rounded up). */
    FULL_DAYS_BELOW_MERGE_WINDOW,
    /** R3: {@code digestDays < 0}. */
    INVALID_DIGEST_DAYS,
    /** New. 05 §4.12: a retention policy row inconsistent with its table, tier or granularity. */
    INVALID_RETENTION_POLICY,
    /** 03 §5.9 ({@code /problems/retention-managed-by-tier}): a table retention that differs from its tier. */
    RETENTION_MANAGED_BY_TIER,

    // ---- correlation and outcome rules: 03 §5.9 O1..O5 ----
    /** O1: malformed outcome rule. */
    INVALID_OUTCOME_RULE,
    /** O2 (checked by the control plane, needs the snapshot): the outcome key is not indexed. */
    OUTCOME_KEY_NOT_INDEXED,
    /** O3: too many, too long or overlapping success/failure values, or values without an outcome key. */
    INVALID_OUTCOME_VALUES,
    /** O4: the final step label is not the step label of one of the type's keys. */
    UNKNOWN_STEP_LABEL,
    /** O5: merge window outside [1 min, 30 d], or not shorter than {@code fullDays - 1 day}. */
    INVALID_MERGE_WINDOW,
    /** New. 05 §4.10: no PRIMARY key, a key used twice, or a step label longer than 64 characters. */
    INVALID_CORRELATION_TYPE,
    /** New. 05 §4.11: a correlation value that is blank or longer than 512 characters. */
    INVALID_CORRELATION_VALUE,
    /** New. 05 §4.11: a business operation violating I4 or the uuidv7/self-merge rules. */
    INVALID_BUSINESS_OPERATION,

    // ---- digest tier (05 §4.14) ----
    /** 03 §5.9: a compaction exclusion pattern that matches every root name. */
    PATTERN_TOO_BROAD,
    /** New. 05 §4.14.2: a digest row violating its invariants. */
    INVALID_DIGEST,
    /** New. 05 §4.14.3: a status change that the compaction state machine does not allow. */
    INVALID_COMPACTION_TRANSITION
}
