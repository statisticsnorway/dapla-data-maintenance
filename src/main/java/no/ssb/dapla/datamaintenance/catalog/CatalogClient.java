package no.ssb.dapla.datamaintenance.catalog;

import com.fasterxml.jackson.annotation.JsonIdentityReference;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@RegisterRestClient(configKey = "catalog")
@Path("/catalog")
public interface CatalogClient {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @JsonIdentityReference
    String list();

}



