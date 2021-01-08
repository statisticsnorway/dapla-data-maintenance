package no.ssb.dapla.datamaintenance.service;


import no.ssb.dapla.datamaintenance.catalog.CatalogClient;
import no.ssb.dapla.datamaintenance.model.CatalogItem;
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
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Path("/api/v1")
@ApplicationScoped
public class DataMaintenanceService {

    private static final JsonBuilderFactory JSON = Json.createBuilderFactory(Collections.emptyMap());
    private static final Logger LOG = LoggerFactory.getLogger(DataMaintenanceService.class);
    private final CatalogClient catalogClient;

    @Inject
    public DataMaintenanceService(Config config) {
        var catalogURL = config.getValue("catalog.url", String.class);
        catalogClient = RestClientBuilder.newBuilder()
                .baseUri(URI.create(catalogURL))
                .build(CatalogClient.class);
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
    public List<DatasetListElement> list(@PathParam("path") String path) {
        LOG.info("Listing datasets on path {}", path);
        try {
            List<CatalogItem> catalogItems = CatalogItem.convertJSON(catalogClient.list());
            return catalogItems.stream()
                    .map(DatasetListElement::convertFromCatalogItem)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            e.printStackTrace(); // TODO return correct error
        }
        return null;
    }

    @DELETE
    @Path("/delete/{datasetpath: .*}")
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
