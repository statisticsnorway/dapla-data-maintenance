package no.ssb.dapla.datamaintenance.service;


import no.ssb.dapla.datamaintenance.config.DataMaintenanceConfigProvider;
import no.ssb.dapla.datamaintenance.model.DatasetListElement;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.RequestScoped;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Path("/api/v1")
@RequestScoped
public class DataMaintenanceService {

    private static final JsonBuilderFactory JSON = Json.createBuilderFactory(Collections.emptyMap());
    private static final Logger LOG = LoggerFactory.getLogger(DataMaintenanceService.class);
    private final DataMaintenanceConfigProvider configProvider;

    @Inject
    public DataMaintenanceService(DataMaintenanceConfigProvider configProvider) {
        this.configProvider = configProvider;
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
        List<DatasetListElement> response = mockDatasetList(path);
        LOG.info("returning " + response);
        return response;
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

    private List<DatasetListElement> mockDatasetList(String path) {
        List<DatasetListElement> list = new ArrayList<>();
        Instant now = Instant.now();
        if (path.equals("temp/skatt")) {
            list.add(new DatasetListElement("skatt.tmp", "Arild Johan Takvam-Borge", now));
            list.add(new DatasetListElement("teststuff", "Hadrien Kohl", now.minusSeconds(1000)));
            list.add(new DatasetListElement("skattelek", "Bjørn-André Skaar", now.minusSeconds(5000)));
        } else if (path.equals("prod/skatt")) {
            list.add(new DatasetListElement("skatt.person", "Arild Johan Takvam-Borge", now));
            list.add(new DatasetListElement("skatt.kommune", "Hadrien Kohl", now.minusSeconds(70000)));
            list.add(new DatasetListElement("skatt.totalt", "Bjørn-André Skaar", now.minusSeconds(40000)));
        }
        return list;
    }
}
