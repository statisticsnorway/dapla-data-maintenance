package no.ssb.dapla.datamaintenance.catalog;

import com.fasterxml.jackson.annotation.JsonIdentityReference;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

@Path("/")
public interface CatalogClient {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @JsonIdentityReference
    @Path("catalog")
    String list();

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("folder")
    IdentifierList folder(
            @QueryParam("prefix") String prefix,
            @QueryParam("version") Instant version,
            @QueryParam("limit") Integer limit);

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("dataset")
    IdentifierList dataset(
            @QueryParam("prefix") String prefix,
            @QueryParam("version") Instant version,
            @QueryParam("limit") Integer limit);

    class IdentifierList {
        public List<Identifier> entries;
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