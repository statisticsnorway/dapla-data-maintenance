package no.ssb.dapla.datamaintenance.storage;

import io.helidon.common.reactive.Multi;
import io.helidon.common.reactive.Single;

/**
 * Two phase delete on object store.
 */
public class StorageService {

    /**
     * Mark the prefix to be deleted
     *
     * @param prefix the prefix to delete.
     */
    public Single<String> markDelete(String prefix) {
        return Single.empty();
    }

    /**
     * Delete the files marked for deletion
     *
     * @param prefix the prefix
     * @return the list of files that are deleted.
     */
    public Multi<String> finishDelete(String prefix) {
        return Multi.empty();
    }

}
