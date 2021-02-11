package no.ssb.dapla.datamaintenance.service;

import io.helidon.webserver.HttpException;
import no.ssb.dapla.data.access.protobuf.DeleteLocationRequest;
import no.ssb.dapla.data.access.protobuf.DeleteLocationResponse;
import no.ssb.dapla.datamaintenance.access.DataAccessService;
import no.ssb.dapla.datamaintenance.access.ProtobufJsonProvider;
import no.ssb.dapla.datamaintenance.catalog.CatalogService;
import no.ssb.dapla.datamaintenance.model.DeleteResponse;
import no.ssb.dapla.datamaintenance.storage.TestableStorageService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.mockserver.model.MediaType;

import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

@ExtendWith(MockServerExtension.class)
class DataMaintenanceServiceTest {

    private static MockServerClient mockServer;
    private TestableStorageService storageService;
    private CatalogService catalogService;
    private DataAccessService accessService;
    private DataMaintenanceService service;

    @BeforeAll
    static void beforeAll(MockServerClient serverClient) throws IOException {
        mockServer = serverClient;
    }

    @BeforeEach
    void setUp() {
        var url = "http://localhost:" + mockServer.getPort();
        catalogService = new CatalogService(url + "/catalog");
        accessService = new DataAccessService(url + "/access");
        // LocalStorageHelper is not thread safe unfortunately.
        storageService = new TestableStorageService(Executors.newSingleThreadExecutor());
        service = new DataMaintenanceService(catalogService, storageService, accessService);
    }

    void mockPathRequest(String path) {
        mockServer.when(request()
                .withPath("/catalog/path")
                .withQueryStringParameter("prefix", path)
                .withQueryStringParameter("limit", "2")
        ).respond(response()
                .withBody("{\"entries\":[{\"path\": \"" + path + "\",\"timestamp\":\"100\"}]}",
                        MediaType.APPLICATION_JSON)
        );
    }

    void mockVersion(String path, Integer... versions) {
        var entries = Stream.of(versions).map(l -> "{\"path\":\"" + path + "\",\"timestamp\":\"" + l + "\"}")
                .collect(Collectors.joining(","));
        mockServer.when(request()
                .withPath("/catalog/version")
                .withQueryStringParameter("path", "/foo/bar")
        ).respond(response()
                .withBody("{\"entries\": [" + entries + "]" +
                          "}\n", MediaType.APPLICATION_JSON));
    }

    void mockDelete(String path, Map<Integer, String> versionURIMap) throws IOException {
        for (Integer version : versionURIMap.keySet()) {
            String URI = versionURIMap.get(version);
            String request = ProtobufJsonProvider.writeAsString(
                    DeleteLocationRequest.newBuilder().setPath(path).setSnapshot(version)
            );

            String response = ProtobufJsonProvider.writeAsString(
                    DeleteLocationResponse.newBuilder()
                            .setAccessAllowed(true)
                            .setParentUri(URI)
                            .setExpirationTime(1234567890)
                            .setAccessToken("some token")
            );

            mockServer.when(request()
                    .withPath("/access/rpc/DataAccessService/deleteLocation")
                    .withBody(request)
            ).respond(response().withBody(
                    response, MediaType.APPLICATION_JSON
            ));
        }
    }

    private void mockFile(String bucket, String... files) throws IOException {
        var fs = storageService.getFileSystem(bucket);
        for (String file : files) {
            try (var stream = Files.newOutputStream(fs.getPath(file))) {
                stream.write(file.getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    void testDelete() throws ExecutionException, InterruptedException, IOException {

        mockPathRequest("/foo/bar");
        mockVersion("/foo/bar", 50, 25, 10);
        mockDelete("/foo/bar", Map.of(
                50, "gs://bucket50/foo/bar/50",
                25, "gs://bucket25/foo/bar/25",
                10, "gs://bucket10/foo/bar/10"
        ));
        mockFile("bucket50", "/foo/bar/50/file1", "/foo/bar/50/file2");
        mockFile("bucket25", "/foo/bar/25/files1", "/foo/bar/25/baz/file2");
        mockFile("bucket10", "/foo/bar/10/files1", "/foo/bar/10/baz/file2");

        var delete = service.delete("/foo/bar", false)
                .toCompletableFuture()
                .get();

        assertThat(delete.datasetPath()).isEqualTo("/foo/bar");
        assertThat(delete.deletedVersions()).extracting(DeleteResponse.DatasetVersion::timestamp)
                .containsExactlyInAnyOrder(
                        Instant.ofEpochMilli(50),
                        Instant.ofEpochMilli(25),
                        Instant.ofEpochMilli(10)
                );
        assertThat(delete.deletedVersions()).containsExactly(
                new DeleteResponse.DatasetVersion(Instant.ofEpochMilli(50), List.of(
                        new DeleteResponse.DeletedFile("gs://bucket50/foo/bar/50/.DELETED", 0L),
                        new DeleteResponse.DeletedFile("gs://bucket50/foo/bar/50/file2", 17L),
                        new DeleteResponse.DeletedFile("gs://bucket50/foo/bar/50/file1", 17L)
                )),
                new DeleteResponse.DatasetVersion(Instant.ofEpochMilli(25), List.of(
                        new DeleteResponse.DeletedFile("gs://bucket25/foo/bar/25/.DELETED", 0L),
                        new DeleteResponse.DeletedFile("gs://bucket25/foo/bar/25/files1", 18L),
                        new DeleteResponse.DeletedFile("gs://bucket25/foo/bar/25/baz/file2", 21L)
                )),
                new DeleteResponse.DatasetVersion(Instant.ofEpochMilli(10), List.of(
                        new DeleteResponse.DeletedFile("gs://bucket10/foo/bar/10/.DELETED", 0L),
                        new DeleteResponse.DeletedFile("gs://bucket10/foo/bar/10/files1", 18L),
                        new DeleteResponse.DeletedFile("gs://bucket10/foo/bar/10/baz/file2", 21L)
                ))
        );

        System.out.println(delete);
    }

    @Test
    void testDeleteWithDatasetAndFolderConflict() {
        mockServer.when(request()
                .withPath("/catalog/path")
                .withQueryStringParameter("prefix", "/foo/datasetAndFolder")
                .withQueryStringParameter("limit", "2")
        ).respond(response("""
                {
                    "entries": [
                        {
                            "path": "/foo/datasetAndFolder",
                            "timestamp": "1610617128787"
                        },
                        {
                            "path": "/foo/datasetAndFolder/baz",
                            "timestamp": "1604071017000"
                        }
                    ]
                }
                """).withHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON));

        assertThatThrownBy(() -> service.delete("/foo/datasetAndFolder", false))
                .isInstanceOf(HttpException.class)
                .hasMessage("the path /foo/datasetAndFolder is also a folder");
    }

    @Test
    void testDeleteVersionNoAccess() {

    }
}