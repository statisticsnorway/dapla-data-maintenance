package no.ssb.dapla.datamaintenance.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import static no.ssb.dapla.datamaintenance.DataMaintenanceApplication.mapper;

public class CatalogItem {

    public static final TypeReference<List<CatalogItem>> TYPE_REFERENCE = new TypeReference<>() {
    };
    private Id id;
    private String type;
    private String valuation;
    private String state;

    public CatalogItem() {
    }

    public void setId(Id id) {
        this.id = id;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setValuation(String valuation) {
        this.valuation = valuation;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Id getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getValuation() {
        return valuation;
    }

    public String getState() {
        return state;
    }

    public class Id {
        String path;
        Date timestamp;

        public Id() {
        }

        public void setPath(String path) {
            this.path = path;
        }

        public void setTimestamp(Date timestamp) {
            this.timestamp = timestamp;
        }

        public String getPath() {
            return path;
        }

        public Date getTimestamp() {
            return timestamp;
        }
    }

    public static List<CatalogItem> convertJSON(String catalogResponse) throws IOException {

        JsonNode jsonNode =
                mapper.readTree(catalogResponse);
        JsonNode catalogs = jsonNode.withArray("catalogs");

        ObjectReader reader = mapper.readerFor(TYPE_REFERENCE);
        return reader.readValue(catalogs.traverse());
    }

}
