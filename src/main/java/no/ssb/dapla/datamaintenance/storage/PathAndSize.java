package no.ssb.dapla.datamaintenance.storage;

import java.nio.file.Path;
import java.util.Objects;

public final class PathAndSize {
    private final Path path;
    private final Long size;

    PathAndSize(Path path, Long size) {
        this.path = path;
        this.size = size;
    }

    public Path getPath() {
        return path;
    }

    public Long getSize() {
        return size;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (PathAndSize) obj;
        return Objects.equals(this.path, that.path) &&
               Objects.equals(this.size, that.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, size);
    }

    @Override
    public String toString() {
        return "PathAndSize[" +
               "path=" + path + ", " +
               "size=" + size + ']';
    }

}
