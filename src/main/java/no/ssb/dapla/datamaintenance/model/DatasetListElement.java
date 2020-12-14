package no.ssb.dapla.datamaintenance.model;

import java.time.Instant;

public class DatasetListElement {
    private String path;
    private String createdBy;
    private Instant createdDate;
    private String type;
    private String valuation;
    private String state;

    public DatasetListElement(String path, String createdBy, Instant createdDate, String type, String valuation, String state) {
        this.path = path;
        this.createdBy = createdBy;
        this.createdDate = createdDate;
        this.type = type;
        this.valuation = valuation;
        this.state = state;
    }

    public String getPath() {
        return path;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public Instant getCreatedDate() {
        return createdDate;
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

    public static DatasetListElement convertFromCatalogItem(CatalogItem cr) {
        return new DatasetListElement(
                cr.getId().getPath(),
                "",
                cr.getId().getTimestamp().toInstant(),
                cr.getType(),
                cr.getValuation(),
                cr.getState()
        );
    }
}
