package no.ssb.dapla.datamaintenance.model;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class DeleteResponse {

    private final String datasetPath;

    public final List<DatasetVersion> deletedVersions;

    public DeleteResponse(String datasetPath, List<DatasetVersion> deletedVersions) {
        this.datasetPath = Objects.requireNonNull(datasetPath);
        this.deletedVersions = Objects.requireNonNull(deletedVersions);
    }

    public Long getTotalSize() {
        return deletedVersions.stream()
                .flatMap(datasetVersion -> datasetVersion.deletedFiles.stream())
                .mapToLong(deletedFile -> deletedFile.size).sum();
    }

    public String getDatasetPath() {
        return datasetPath;
    }

    public static class DatasetVersion {

        public final Instant timestamp;
        public final List<DeletedFile> deletedFiles;

        public DatasetVersion(Instant timestamp, List<DeletedFile> deletedFiles) {
            this.timestamp = timestamp;
            this.deletedFiles = deletedFiles;
        }
    }

    public static class DeletedFile {

        public final String uri;
        public final Long size;

        public DeletedFile(String uri, Long size) {
            this.uri = Objects.requireNonNull(uri);
            this.size =  Objects.requireNonNull(size);
        }

        public DeletedFile(Path path, Long size) {
            this(path.toString(), size);
        }
    }
}
