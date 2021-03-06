package no.ssb.dapla.datamaintenance.catalog;

import no.ssb.dapla.catalog.protobuf.DeleteDatasetRequest;
import no.ssb.dapla.catalog.protobuf.DeleteDatasetResponse;
import no.ssb.dapla.datamaintenance.access.ProtobufJsonProvider;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

@Path("/")
@RegisterProvider(InstantConverterProvider.class)
@RegisterProvider(ProtobufJsonProvider.class)
public interface CatalogClient {

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("folder")
    CompletionStage<IdentifierList> folderAsync(@QueryParam("prefix") String prefix,
                                                @QueryParam("version") Instant version,
                                                @QueryParam("limit") Integer limit);

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("dataset")
    CompletionStage<IdentifierList> datasetAsync(@QueryParam("prefix") String prefix,
                                                 @QueryParam("version") Instant version,
                                                 @QueryParam("limit") Integer limit);

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("path")
    CompletionStage<IdentifierList> pathAsync(@QueryParam("prefix") String prefix,
                                              @QueryParam("version") Instant version,
                                              @QueryParam("limit") Integer limit
    );

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("version")
    CompletionStage<IdentifierList> versionAsync(@QueryParam("path") String prefix,
                                              @QueryParam("limit") Integer limit
    );

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path("rpc/CatalogService/delete")
    CompletionStage<DeleteDatasetResponse> deleteAsync(DeleteDatasetRequest request,
                                                       @HeaderParam("Authorization") String authorization);

    class IdentifierList {
        public List<Identifier> entries = List.of();
    }

    class Identifier {

        public String path;
        public Long timestamp;

        static Identifier of(String path, Long timestamp) {
            var id = new Identifier();
            id.path = path;
            id.timestamp = timestamp;
            return id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Identifier that = (Identifier) o;
            return Objects.equals(path, that.path) && Objects.equals(timestamp, that.timestamp);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, timestamp);
        }
    }
}