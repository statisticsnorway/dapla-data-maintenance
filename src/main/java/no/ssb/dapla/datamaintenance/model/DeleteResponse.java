package no.ssb.dapla.datamaintenance.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public record DeleteResponse(String datasetPath, List<DatasetVersion> deletedVersions, Boolean dryRun) {

    public Long getTotalSize() {
        return deletedVersions.stream()
                .flatMap(datasetVersion -> datasetVersion.deletedFiles.stream())
                .mapToLong(deletedFile -> deletedFile.size).sum();
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public static record DatasetVersion(Instant timestamp,
                                        List<DeletedFile> deletedFiles) {
    }

    @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
    public static record DeletedFile(String uri, Long size) {

        public DeletedFile(Path uri, Long size) {
            this(uri.toString(), size);
        }
    }
}
