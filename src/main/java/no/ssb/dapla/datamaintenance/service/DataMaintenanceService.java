package no.ssb.dapla.datamaintenance.service;


import io.helidon.common.http.Http;
import io.helidon.common.reactive.Multi;
import io.helidon.common.reactive.Single;
import io.helidon.webserver.HttpException;
import no.ssb.dapla.data.access.protobuf.DeleteLocationResponse;
import no.ssb.dapla.datamaintenance.access.DataAccessService;
import no.ssb.dapla.datamaintenance.catalog.CatalogService;
import no.ssb.dapla.datamaintenance.model.DatasetListElement;
import no.ssb.dapla.datamaintenance.model.DeleteResponse;
import no.ssb.dapla.datamaintenance.storage.StorageService;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import static no.ssb.dapla.datamaintenance.catalog.CatalogClient.Identifier;

@Path("/api/v1")
@ApplicationScoped
public class DataMaintenanceService {

    private static final JsonBuilderFactory JSON = Json.createBuilderFactory(Collections.emptyMap());
    private static final Logger LOG = LoggerFactory.getLogger(DataMaintenanceService.class);

    private final CatalogService catalogService;
    private final StorageService storageService;
    private final DataAccessService dataAccessService;

    @Inject
    public DataMaintenanceService(CatalogService catalogService, StorageService storageService, DataAccessService dataAccessService) {
        this.catalogService = Objects.requireNonNull(catalogService);
        this.storageService = Objects.requireNonNull(storageService);
        this.dataAccessService = Objects.requireNonNull(dataAccessService);
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
    public CompletionStage<DeleteResponse> delete(@PathParam("path") String datasetPath, @QueryParam("dry-run") Boolean dryRun) {

        // TODO: Query/Auth parameters
        String JWT = "";
        String userId = "Donald Trump";
        Instant now = Instant.now();

        // Make sure the dataset is not also a folder.
        Single<Map<Identifier, DeleteLocationResponse>> tokens = catalogService.getDatasetVersions(datasetPath, Integer.MAX_VALUE)
                .flatMap(identifier ->
                        dataAccessService.getDeleteToken(identifier.path, identifier.timestamp, JWT)
                                .map(r -> new AbstractMap.SimpleEntry<>(identifier, r)))
                .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()));

        if (!catalogService.isOnlyDataset(datasetPath, now).await()) {
            tokens.cancel();
            throw new HttpException("the path " + datasetPath + " is also a folder");
        }

        // Mark the versions to be deleted first.
        tokens.flatMapIterable(Map::entrySet).flatMap(e -> {
            var parentUri = URI.create(e.getValue().getParentUri());
            var token = e.getValue().getAccessTokenBytes().newInput();
            return storageService.markDelete(parentUri, token);
        }).collectList().await();

        // Asynchronously call
        return tokens.flatMapIterable(Map::entrySet).flatMap(e -> {
            var parentUri = URI.create(e.getValue().getParentUri());
            var token = e.getValue().getAccessTokenBytes().newInput();
            var version = Instant.ofEpochMilli(e.getKey().timestamp);
            return storageService.finishDelete(parentUri, token).flatMap(path ->
                    storageService.getSize(path).onErrorResume(throwable -> -1L)
                            .map(size -> new DeleteResponse.DeletedFile(path, size))
            ).collectList().map(deletedFiles -> new DeleteResponse.DatasetVersion(version, deletedFiles));
        }).collectList().map(versions -> new DeleteResponse(datasetPath, versions));
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
