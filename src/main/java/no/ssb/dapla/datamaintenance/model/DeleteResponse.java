package no.ssb.dapla.datamaintenance.model;

import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public final class DeleteResponse {

    private final String datasetPath;
    private final Set<DatasetVersion> deletedVersions;
    private final Boolean dryRun;

    public DeleteResponse(String datasetPath, Collection<DatasetVersion> deletedVersions, Boolean dryRun) {
        this.datasetPath = datasetPath;
        this.deletedVersions = new HashSet<>(deletedVersions);
        this.dryRun = dryRun;
    }

    public String getDatasetPath() {
        return datasetPath;
    }

    public Set<DatasetVersion> getDeletedVersions() {
        return deletedVersions;
    }

    public Boolean getDryRun() {
        return dryRun;
    }

    public Long getTotalSize() {
        return deletedVersions.stream()
                .flatMap(datasetVersion -> datasetVersion.deletedFiles.stream())
                .mapToLong(deletedFile -> deletedFile.size).sum();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (DeleteResponse) obj;
        return Objects.equals(this.datasetPath, that.datasetPath) &&
               Objects.equals(this.deletedVersions, that.deletedVersions) &&
               Objects.equals(this.dryRun, that.dryRun);
    }

    @Override
    public int hashCode() {
        return Objects.hash(datasetPath, deletedVersions, dryRun);
    }

    @Override
    public String toString() {
        return "DeleteResponse[" +
               "datasetPath=" + datasetPath + ", " +
               "deletedVersions=" + deletedVersions + ", " +
               "dryRun=" + dryRun + ']';
    }

    public static final class DatasetVersion {

        public Instant getTimestamp() {
            return timestamp;
        }

        public Set<DeletedFile> getDeletedFiles() {
            return deletedFiles;
        }

        private final Instant timestamp;
        private final Set<DeletedFile> deletedFiles;

        public DatasetVersion(Instant timestamp,
                              Collection<DeletedFile> deletedFiles) {
            this.timestamp = timestamp;
            this.deletedFiles = new HashSet<>(deletedFiles);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (DatasetVersion) obj;
            return Objects.equals(this.timestamp, that.timestamp) &&
                   Objects.equals(this.deletedFiles, that.deletedFiles);
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, deletedFiles);
        }

        @Override
        public String toString() {
            return "DatasetVersion[" +
                   "timestamp=" + timestamp + ", " +
                   "deletedFiles=" + deletedFiles + ']';
        }

    }

    public static final class DeletedFile {
        public String getUri() {
            return uri;
        }

        public Long getSize() {
            return size;
        }

        private final String uri;
        private final Long size;

        public DeletedFile(String uri, Long size) {
            this.uri = uri;
            this.size = size;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (DeletedFile) obj;
            return Objects.equals(this.uri, that.uri) &&
                   Objects.equals(this.size, that.size);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uri, size);
        }

        @Override
        public String toString() {
            return "DeletedFile[" +
                   "uri=" + uri + ", " +
                   "size=" + size + ']';
        }


    }
}
