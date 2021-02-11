package no.ssb.dapla.datamaintenance.service;

import io.helidon.webserver.HttpException;
import no.ssb.dapla.datamaintenance.access.DataAccessService;
import no.ssb.dapla.datamaintenance.catalog.CatalogService;
import no.ssb.dapla.datamaintenance.storage.TestableStorageService;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.mockserver.model.MediaType;

import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

@ExtendWith(MockServerExtension.class)
class DataMaintenanceServiceTest {

    private static MockWebServer catalogServer;
    private static MockWebServer accessServer;
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
        storageService = new TestableStorageService();
        service = new DataMaintenanceService(catalogService, storageService, accessService);
    }

    @Test
    void testDelete() throws ExecutionException, InterruptedException {
        mockServer.when(request()
                .withPath("/catalog/path")
                .withQueryStringParameter("prefix", "/foo/bar")
                .withQueryStringParameter("limit", "2")
        ).respond(response()
                .withBody("""
                        {
                            "entries": [
                                {
                                    "path": "/foo/bar",
                                    "timestamp": "1610617128787"
                                }
                            ]
                        }
                        """, MediaType.APPLICATION_JSON));

        var delete = service.delete("/foo/bar", false)
                .toCompletableFuture()
                .get();
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