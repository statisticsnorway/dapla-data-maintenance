package no.ssb.dapla.datamaintenance.service;


import io.helidon.common.http.Http;
import io.helidon.common.reactive.Multi;
import io.helidon.common.reactive.Single;
import io.helidon.webserver.HttpException;
import no.ssb.dapla.datamaintenance.access.DataAccessService;
import no.ssb.dapla.datamaintenance.access.UserAccessService;
import no.ssb.dapla.datamaintenance.catalog.CatalogClient;
import no.ssb.dapla.datamaintenance.catalog.CatalogService;
import no.ssb.dapla.datamaintenance.model.DatasetListElement;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.rest.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static no.ssb.dapla.datamaintenance.catalog.CatalogClient.*;

@Path("/api/v1")
@ApplicationScoped
public class DataMaintenanceService {

    private static final JsonBuilderFactory JSON = Json.createBuilderFactory(Collections.emptyMap());
    private static final Logger LOG = LoggerFactory.getLogger(DataMaintenanceService.class);
    private final CatalogService catalogService;

    @Inject
    public DataMaintenanceService(Config config) {
        var catalogURL = config.getValue("catalog.url", String.class);
        var catalogClient = RestClientBuilder.newBuilder()
                .baseUri(URI.create(catalogURL))
                .build(CatalogClient.class);
        catalogService = new CatalogService(catalogClient);
    }

    // TODO: Review the model here.
    public static DatasetListElement toFolder(Identifier identifier) {
        return new DatasetListElement(
                identifier.path,
                "TODO",
                Instant.ofEpochMilli(identifier.timestamp),
                "TODO", "TODO", "TODO",
                1 // Note the 1 indicating dataset.
        );
    }

    // TODO: Review the model here.
    public static DatasetListElement toDataset(Identifier identifier) {
        return new DatasetListElement(
                identifier.path,
                "TODO",
                Instant.ofEpochMilli(identifier.timestamp),
                "TODO", "TODO", "TODO",
                0 // Note the 0 indicating dataset.
        );
    }

    @GET
    @Path("/list/{path: .*}")
    @Operation(summary = "List datasets",
            description = "List datasets with metadata on given path")
    @APIResponse(
            description = "List of datasets with metadata on given path",
            content = @Content(
                    mediaType = MediaType.APPLICATION_JSON,
                    schema = @Schema(implementation = DatasetListElement.class)
            )
    )
    @Produces(MediaType.APPLICATION_JSON)
    public CompletionStage<List<DatasetListElement>> list(@PathParam("path") String path) throws ExecutionException, InterruptedException {
        LOG.info("Listing datasets on path {}", path);

        var now = Instant.now();
        var limit = 1000;

        // We fire three call to catalog: path to check that the path exists, folder to list the folders
        // under the path, and dataset to list the datasets under the path.
        // Note that the calls are made simultaneously regardless of the result of the path call.

        var folders = catalogService.getFolders(path, now, limit)
                .map(DataMaintenanceService::toFolder);

        var datasets = catalogService.getDatasets(path, now, limit)
                .map(DataMaintenanceService::toDataset);

        return catalogService.doesPathExist(path, now)
                .flatMapSingle(exists -> {
                    // Check that the path exists.
                    if (exists) {
                        // Could also throw here.
                        return Single.error(new HttpException("Cannot access '" + path + "': No such dataset or folder",
                                Http.Status.NOT_FOUND_404));
                    } else {
                        // Merge the two calls.
                        return Multi.concat(folders, datasets).collectList();
                    }
                });
    }

    @DELETE
    @Path("/delete/{path: .*}")
    @Operation(summary = "Delete a dataset",
            description = "Delete a dataset from given path"
    )
    @APIResponses(
            value = {
                    @APIResponse(
                            responseCode = "202",
                            description = "Dataset deleted successfully",
                            content = @Content(mediaType = MediaType.TEXT_PLAIN)
                    ),
                    @APIResponse(
                            responseCode = "404",
                            description = "Dataset not found",
                            content = @Content(mediaType = MediaType.TEXT_PLAIN)
                    )
            }
    )
    public Response delete(@PathParam("datasetpath") String dataset) {
        // mock a respponse
        if (dataset.equals("temp/skatt/skatt.tmp")) {
            LOG.info("Deleting dataset {}", dataset);
            return Response.status(Response.Status.ACCEPTED)
                    .entity("Dataset " + dataset + " deleted")
                    .build();
        }
        LOG.info("Dataset {} not found", dataset);
        return Response.status(Response.Status.NOT_FOUND)
                .entity("Dataset " + dataset + " not found").build();
    }

    @Path("/test")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public JsonObject test() {
        LOG.debug("Received request for /test");
        return JSON.createObjectBuilder()
                .add("message", "Server is up and running")
                .build();
    }
}
