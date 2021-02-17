package no.ssb.dapla.datamaintenance.service;

import io.helidon.common.http.Http;
import io.helidon.common.reactive.Single;
import io.helidon.webserver.HttpException;
import no.ssb.dapla.data.access.protobuf.DeleteLocationRequest;
import no.ssb.dapla.data.access.protobuf.DeleteLocationResponse;
import no.ssb.dapla.datamaintenance.access.DataAccessService;
import no.ssb.dapla.datamaintenance.access.ProtobufJsonProvider;
import no.ssb.dapla.datamaintenance.catalog.CatalogService;
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
import static no.ssb.dapla.datamaintenance.model.DeleteResponse.DatasetVersion;
import static no.ssb.dapla.datamaintenance.model.DeleteResponse.DeletedFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

@ExtendWith(MockServerExtension.class)
class DataMaintenanceServiceTest {

    private static MockServerClient mockServer;
    private TestableStorageService storageService;
    private DataMaintenanceService service;

    @BeforeAll
    static void beforeAll(MockServerClient serverClient) {
        mockServer = serverClient;
    }

    @BeforeEach
    void setUp() {
        var url = "http://localhost:" + mockServer.getPort();
        CatalogService catalogService = new CatalogService(url + "/catalog");
        DataAccessService accessService = new DataAccessService(url + "/access");
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

    void mockUnauthorizedDeleteToken(String path, Map<Integer, String> versionURIMap) throws IOException {
        mockDeleteToken(path, versionURIMap, false);
    }

    void mockDeleteToken(String path, Map<Integer, String> versionURIMap, Boolean hasAccess) throws IOException {
        for (Integer version : versionURIMap.keySet()) {
            String URI = versionURIMap.get(version);
            String request = ProtobufJsonProvider.writeAsString(
                    DeleteLocationRequest.newBuilder().setPath(path).setSnapshot(version)
            );

            var protoResp = DeleteLocationResponse.newBuilder()
                    .setAccessAllowed(hasAccess)
                    .setParentUri(URI);

            if (hasAccess) {
                protoResp.setExpirationTime(1234567890)
                        .setAccessToken("some token");
            }

            String response = ProtobufJsonProvider.writeAsString(protoResp);

            mockServer.when(request()
                    .withPath("/access/rpc/DataAccessService/deleteLocation")
                    .withBody(request)
            ).respond(response().withBody(
                    response, MediaType.APPLICATION_JSON
            ));
        }
    }

    void mockAuthorizedDeleteToken(String path, Map<Integer, String> versionURIMap) throws IOException {
        mockDeleteToken(path, versionURIMap, true);
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
        mockAuthorizedDeleteToken("/foo/bar", Map.of(
                50, "gs://bucket50/foo/bar/50",
                25, "gs://bucket25/foo/bar/25",
                10, "gs://bucket10/foo/bar/10"
        ));
        mockFile("bucket50", "/foo/bar/50/file1", "/foo/bar/50/file2");
        mockFile("bucket25", "/foo/bar/25/files1", "/foo/bar/25/baz/file2");
        mockFile("bucket10", "/foo/bar/10/files1", "/foo/bar/10/baz/file2");

        var delete = service.delete("/foo/bar", false, "token")
                .toCompletableFuture()
                .get();

        assertThat(delete.getDatasetPath()).isEqualTo("/foo/bar");
        assertThat(delete.getTotalSize()).isEqualTo(112L);
        assertThat(delete.getDeletedVersions()).containsExactly(
                new DatasetVersion(Instant.ofEpochMilli(50), List.of(
                        new DeletedFile("gs://bucket50/foo/bar/50/.DELETED", 0L),
                        new DeletedFile("gs://bucket50/foo/bar/50/file2", 17L),
                        new DeletedFile("gs://bucket50/foo/bar/50/file1", 17L)
                )),
                new DatasetVersion(Instant.ofEpochMilli(25), List.of(
                        new DeletedFile("gs://bucket25/foo/bar/25/.DELETED", 0L),
                        new DeletedFile("gs://bucket25/foo/bar/25/files1", 18L),
                        new DeletedFile("gs://bucket25/foo/bar/25/baz/file2", 21L)
                )),
                new DatasetVersion(Instant.ofEpochMilli(10), List.of(
                        new DeletedFile("gs://bucket10/foo/bar/10/.DELETED", 0L),
                        new DeletedFile("gs://bucket10/foo/bar/10/files1", 18L),
                        new DeletedFile("gs://bucket10/foo/bar/10/baz/file2", 21L)
                ))
        );
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

        assertThatThrownBy(() -> service.delete("/foo/datasetAndFolder", false, "token"))
                .isInstanceOf(HttpException.class)
                .hasMessage("the path /foo/datasetAndFolder is both a dataset and a folder. " +
                            "You must delete the folder to be able to delete the dataset.")
                .extracting(throwable -> ((HttpException) throwable).status())
                .isEqualTo(Http.Status.CONFLICT_409);
    }

    @Test
    void testDeleteVersionNoAccess() throws IOException {
        mockPathRequest("/foo/bar");
        mockVersion("/foo/bar", 50, 25, 10);
        mockAuthorizedDeleteToken("/foo/bar", Map.of(
                50, "gs://bucket50/foo/bar/50"
        ));
        mockUnauthorizedDeleteToken("/foo/bar", Map.of(
                25, "gs://bucket25/foo/bar/25",
                10, "gs://bucket10/foo/bar/10"
        ));

        mockFile("bucket50", "/foo/bar/50/file1", "/foo/bar/50/file2");
        mockFile("bucket25", "/foo/bar/25/file1", "/foo/bar/25/baz/file2");
        mockFile("bucket10", "/foo/bar/10/file1", "/foo/bar/10/baz/file2");

        assertThatThrownBy(() -> Single.create(service.delete("/foo/bar", false, "token")).await())
                .hasMessageContaining("missing delete access for versions")
                .hasMessageContaining("/foo/bar, 1970-01-01T00:00:00.025Z")
                .hasMessageContaining("/foo/bar, 1970-01-01T00:00:00.010Z")
                .isInstanceOf(HttpException.class)
                .extracting(throwable -> ((HttpException) throwable).status())
                .isEqualTo(Http.Status.FORBIDDEN_403);

        assertThat(Files.walk(storageService.getFileSystem("bucket50").getPath("/")))
                .extracting(path -> path.toUri().toString())
                .contains(
                        "gs://bucket50/foo/bar/50/file1",
                        "gs://bucket50/foo/bar/50/file2"
                );

        assertThat(Files.walk(storageService.getFileSystem("bucket25").getPath("/")))
                .extracting(path -> path.toUri().toString())
                .contains(
                        "gs://bucket25/foo/bar/25/file1",
                        "gs://bucket25/foo/bar/25/baz/file2"
                );
        assertThat(Files.walk(storageService.getFileSystem("bucket10").getPath("/")))
                .extracting(path -> path.toUri().toString())
                .contains(
                        "gs://bucket10/foo/bar/10/file1",
                        "gs://bucket10/foo/bar/10/baz/file2"
                );
    }
}