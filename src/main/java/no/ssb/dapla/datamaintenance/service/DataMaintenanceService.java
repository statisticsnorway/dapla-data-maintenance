package no.ssb.dapla.datamaintenance.service;


import no.ssb.dapla.datamaintenance.config.DataMaintenanceConfigProvider;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.json.Json;
import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Collections;

@Path("/")
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
    @Path("/test2")
    @Operation(summary = "Test summary",
            description = "Test description")
    @APIResponse(
            description = "Response description",
            content = @Content(
                    mediaType = "application/json",
                    schema = @Schema(implementation = SomeModel.class)
            )
    )
    @Produces(MediaType.APPLICATION_JSON)
    public SomeModel openApiTest() {
        var response = new SomeModel("Hello" + configProvider.toString());
        LOG.info("returning " + response);
        return response;
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

    public static class SomeModel {
        private String message;

        private SomeModel(String message) {
            this.message = message;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }
}
