package ru.aasmc.tracing.collector.domain.enrichment;

/**
 * Reference to a secret held outside the database: a Vault path or a Kubernetes secret (05 §4.6). Never the secret
 * itself, so secrets never appear in the database, in API responses or in {@code toString()}.
 *
 * <p>Invariant ({@code INVALID_CONNECTION}): matches {@code ^(vault|k8s):.+}.
 *
 * @param uri the reference, e.g. {@code vault:secret/data/tcs/sms-gateway#jaas}
 */
public record SecretRef(String uri) {

    public boolean isVault() {
        throw new UnsupportedOperationException();
    }
}
