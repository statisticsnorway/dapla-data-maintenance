package no.ssb.dapla.datamaintenance.access;

import io.helidon.common.reactive.Single;
import io.helidon.config.Config;
import no.ssb.dapla.datamaintenance.model.DatasetListElement;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import javax.inject.Inject;
import java.net.URI;

public class UserAccessService {

    private final UserAccessClient client;

    @Inject
    public UserAccessService(Config config) {
        this(config.get("user-access.url").asString().get());
    }

    public UserAccessService(String url) {
        client = RestClientBuilder.newBuilder()
                .baseUri(URI.create(url))
                .build(UserAccessClient.class);
    }

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
