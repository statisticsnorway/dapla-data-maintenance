package no.ssb.dapla.datamaintenance.access;

import io.helidon.common.reactive.Single;
import io.helidon.config.Config;
import no.ssb.dapla.data.access.protobuf.DeleteLocationRequest;
import no.ssb.dapla.data.access.protobuf.DeleteLocationResponse;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.net.URI;

@ApplicationScoped
public class DataAccessService {

    private DataAccessClient client;

    @Inject
    public DataAccessService(Config config) {
        this(config.get("user-access.url").asString().get());
    }

    public DataAccessService(String url) {
        client = RestClientBuilder.newBuilder()
                .baseUri(URI.create(url))
                .build(DataAccessClient.class);
    }

    public Single<DeleteLocationResponse> getDeleteToken(String path, Long version, String token) {
        var request = DeleteLocationRequest.newBuilder()
                .setPath(path)
                .setSnapshot(version)
                .build();
        if (!token.startsWith("Bearer ")) {
            token = "Bearer " + token;
        }
        return Single.create(client.deleteLocation(request, token));
    }
}
