package no.ssb.dapla.datamaintenance.access;

import io.helidon.common.reactive.Single;
import no.ssb.dapla.datamaintenance.model.DatasetListElement;

public class UserAccessService {

    public Single<Boolean> hasAccess(
            String userId,
            String privilege,
            String valuation,
            String state,
            String path
    ) {
        return Single.empty();
    }

    public Single<Boolean> hasAccess(
            String userId,
            String privilege,
            DatasetListElement dataset
    ) {
        return hasAccess(userId, privilege, dataset.getValuation(), dataset.getState(), dataset.getPath()
        );
    }

    public Single<Boolean> hasDeleteAccess(
            String userId,
            DatasetListElement dataset
    ) {
        // TODO: Enum for privilege.
        return hasAccess(userId, "DELETE", dataset.getValuation(), dataset.getState(), dataset.getPath()
        );
    }


}
