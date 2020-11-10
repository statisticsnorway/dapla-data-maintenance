package no.ssb.dapla.datamaintenance.service;


import no.ssb.dapla.datamaintenance.config.DataMaintenanceConfigProvider;
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
    private final DataMaintenanceConfigProvider configProvider;
    private static final Logger LOG = LoggerFactory.getLogger(DataMaintenanceService.class);

    @Inject
    public DataMaintenanceService(DataMaintenanceConfigProvider configProvider) {
        this.configProvider = configProvider;
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
