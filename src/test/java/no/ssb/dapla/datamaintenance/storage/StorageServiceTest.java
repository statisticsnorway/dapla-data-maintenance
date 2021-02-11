package no.ssb.dapla.datamaintenance.storage;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

class StorageServiceTest {

    @Test
    void testThatSchemeAndBucketAreRemoved() {
        var gsPath = StorageService.removeSchemeAndHost(URI.create("gs://bucket-name/foo/bar"));
        assertThat(gsPath.toASCIIString()).isEqualTo("/foo/bar");

        var filePath = StorageService.removeSchemeAndHost(URI.create("file:///foo/bar"));
        assertThat(filePath.toASCIIString()).isEqualTo("file:///foo/bar");
    }

    @Test
    void testDeleteWorks() throws IOException {
        var service = new TestableStorageService();
        var fs = service.getFileSystem("fake-bucket-1");

        // Setup some files.
        Files.createDirectories(fs.getPath("/foo/bar"));
        Files.createFile(fs.getPath("/foo/bar/file1"));
        Files.createFile(fs.getPath("/foo/bar/file2"));

        Files.createDirectories(fs.getPath("/bar/foo"));
        Files.createFile(fs.getPath("/bar/foo/file1"));
        Files.createFile(fs.getPath("/bar/foo/file2"));

        Files.createDirectories(fs.getPath("/foo/bar/baz"));
        Files.createFile(fs.getPath("/foo/bar/baz/file1"));
        Files.createFile(fs.getPath("/foo/bar/baz/file2"));

        var deleteMarker = service.markDelete(URI.create("gs://fake-bucket-1/foo/bar"), InputStream.nullInputStream())
                .await();

        assertThat(deleteMarker).asString().isEqualTo("/foo/bar/.DELETED");

        assertThat(Files.exists(fs.getPath("/foo/bar/file1"))).isTrue();
        assertThat(Files.exists(fs.getPath("/foo/bar/file2"))).isTrue();

        assertThat(Files.exists(fs.getPath("/bar/foo/file1"))).isTrue();
        assertThat(Files.exists(fs.getPath("/bar/foo/file2"))).isTrue();

        assertThat(Files.exists(fs.getPath("/foo/bar/baz/file1"))).isTrue();
        assertThat(Files.exists(fs.getPath("/foo/bar/baz/file2"))).isTrue();

        var deletedFiles = service.finishDelete(URI.create("gs://fake-bucket-1/foo/bar"), InputStream.nullInputStream())
                .map(path -> path.toString()).collectList().await();

        assertThat(deletedFiles).containsExactly(
                "/foo/bar/file1",
                "/foo/bar/file2",
                "/foo/bar/.DELETED",
                "/foo/bar/baz/file1",
                "/foo/bar/baz/file2"
        );
    }

}