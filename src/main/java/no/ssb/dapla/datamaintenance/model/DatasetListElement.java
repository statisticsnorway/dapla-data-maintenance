package no.ssb.dapla.datamaintenance.model;

import java.time.Instant;

public class DatasetListElement {
    private String name;
    private String createdBy;
    private Instant createdDate;

    public DatasetListElement(String name, String createdBy, Instant createdDate) {
        this.name = name;
        this.createdBy = createdBy;
        this.createdDate = createdDate;
    }

    public String getName() {
        return name;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }
}
