package no.ssb.dapla.datamaintenance.access;

import io.helidon.common.reactive.Single;
import io.helidon.config.Config;
import no.ssb.dapla.data.access.protobuf.DeleteLocationRequest;
import no.ssb.dapla.data.access.protobuf.DeleteLocationResponse;
import no.ssb.dapla.datamaintenance.ExceptionConverter;
import org.eclipse.microprofile.rest.client.RestClientBuilder;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.net.URI;

@ApplicationScoped
public class DataAccessService {

    private DataAccessClient client;

    @Inject
    public DataAccessService(Config config) {
        this(config.get("dataaccess.url").asString().get());
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
        String finalToken = token;
        return Single.defer(() ->
                Single.create(client.deleteLocation(request, finalToken))
                        .onError(throwable -> new ExceptionConverter("failed to get token"))
        );
    }
}
