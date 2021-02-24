package no.ssb.dapla.datamaintenance.catalog;

import io.helidon.common.reactive.Multi;
import io.helidon.common.reactive.Single;
import io.helidon.config.Config;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetRequest;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetResponse;
import no.ssb.dapla.datamaintenance.ExceptionConverter;
import no.ssb.dapla.datamaintenance.catalog.CatalogClient.Identifier;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.net.URI;
import java.time.Instant;

/**
 * Wrap catalog client.
 */
@ApplicationScoped
public class CatalogService {

    private final CatalogClient client;

    @Inject
    public CatalogService(Config config) {
        this(config.get("catalog.url").asString().get());
    }

    public CatalogService(String url) {
        client = RestClientBuilder.newBuilder()
                .baseUri(URI.create(url))
                .build(CatalogClient.class);
    }

    public CatalogService(CatalogClient client) {
        this.client = client;
    }

    public Single<Boolean> doesPathExist(String prefix, Instant version) {
        return getPath(prefix, version, 1).collectList()
                .map(identifiers -> !identifiers.isEmpty());
    }

    public Single<Boolean> isOnlyDataset(String prefix, Instant version) {
        return getPath(prefix, version, 2).collectList()
                .map(identifiers -> identifiers.size() == 1);
    }

    public Multi<Identifier> getPath(String prefix, Instant version, Integer limit) {
        return Single.create(client.pathAsync(prefix, version, limit))
                .flatMapIterable(identifierList -> identifierList.entries)
                .onError(throwable -> new ExceptionConverter("failed to get paths"));
    }

    public Multi<Identifier> getDatasets(String prefix, Instant version, Integer limit) {
        return Single.create(client.datasetAsync(prefix, version, limit))
                .flatMapIterable(identifierList -> identifierList.entries)
                .onError(throwable -> new ExceptionConverter("failed to get datasets"));
    }

    public Multi<Identifier> getFolders(String prefix, Instant version, Integer limit) {
        return Single.create(client.folderAsync(prefix, version, limit))
                .flatMapIterable(identifierList -> identifierList.entries)
                .onError(throwable -> new ExceptionConverter("failed to get folders"));
    }

    public Multi<Identifier> getDatasetVersions(String path, Integer limit) {
        return Single.create(client.versionAsync(path, limit))
                .flatMapIterable(identifierList -> identifierList.entries)
                .onError(throwable -> new ExceptionConverter("failed to get versions"));
    }

    public Single<DeleteDatasetResponse> deleteDatasetVersion(String path, Instant version, String token) {
        DeleteDatasetRequest request = DeleteDatasetRequest.newBuilder()
                .setPath(path)
                .setTimestamp(version.toEpochMilli())
                .build();
        if (!token.startsWith("Bearer ")) {
            token = "Bearer " + token;
        }
        return Single.create(client.deleteAsync(request, token));

    }
}
