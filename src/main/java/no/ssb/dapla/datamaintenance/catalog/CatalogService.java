package no.ssb.dapla.datamaintenance.catalog;

import io.helidon.common.reactive.Multi;
import io.helidon.common.reactive.Single;
import no.ssb.dapla.datamaintenance.catalog.CatalogClient.Identifier;

import java.time.Instant;

/**
 * Wrap catalog client.
 */
public class CatalogService {

    private final CatalogClient client;


    public CatalogService(CatalogClient client) {
        this.client = client;
    }

    public Single<Boolean> doesPathExist(String prefix, Instant version) {
        return getPath(prefix, version, 1).collectList()
                .map(identifiers -> !identifiers.isEmpty());
    }

    public Multi<Identifier> getPath(String prefix, Instant version, Integer limit) {
        return Single.create(client.pathAsync(prefix, version, limit))
                .flatMapIterable(identifierList -> identifierList.entries);
    }

    public Multi<Identifier> getDatasets(String prefix, Instant version, Integer limit) {
        return Single.create(client.datasetAsync(prefix, version, limit))
                .flatMapIterable(identifierList -> identifierList.entries);
    }

    public Multi<Identifier> getFolders(String prefix, Instant version, Integer limit) {
        return Single.create(client.folderAsync(prefix, version, limit))
                .flatMapIterable(identifierList -> identifierList.entries);
    }

    public Multi<Identifier> getDatasetVersions(String path, Integer limit) {
        return Multi.empty();
    }
}
