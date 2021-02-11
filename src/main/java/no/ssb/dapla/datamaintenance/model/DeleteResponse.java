package no.ssb.dapla.datamaintenance.model;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public record DeleteResponse(String datasetPath, List<DatasetVersion> deletedVersions) {

    public Long getTotalSize() {
        return deletedVersions.stream()
                .flatMap(datasetVersion -> datasetVersion.deletedFiles.stream())
                .mapToLong(deletedFile -> deletedFile.size).sum();
    }

    public static record DatasetVersion(Instant timestamp,
                                        List<DeletedFile> deletedFiles) {
    }

    public static record DeletedFile(String uri, Long size) {

        public DeletedFile(Path uri, Long size) {
            this(uri.toString(), size);
        }
    }
}
