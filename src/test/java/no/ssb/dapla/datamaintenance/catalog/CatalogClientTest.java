package no.ssb.dapla.datamaintenance.catalog;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.eclipse.microprofile.rest.client.RestClientBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.ws.rs.core.HttpHeaders;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ExecutionException;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.assertj.core.api.Assertions.assertThat;

class CatalogClientTest {

    private static MockWebServer server;
    private CatalogClient client;

    @BeforeAll
    static void beforeAll() throws IOException {
        server = new MockWebServer();
        server.start();
    }

    @BeforeEach
    void setUp() {
        client = RestClientBuilder.newBuilder()
                .baseUri(server.url("/").uri())
                .build(CatalogClient.class);
    }

    @Test
    void testListFolder() throws InterruptedException, ExecutionException {

        server.enqueue(new MockResponse()
                .setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
                .setResponseCode(200)
                .setBody("""
                        {
                            "entries": [
                                {
                                    "path": "felles",
                                    "timestamp": "1610617128787"
                                },
                                {
                                    "path": "kilde",
                                    "timestamp": "1604071017000"
                                },
                                {
                                    "path": "produkt",
                                    "timestamp": "1608128434194"
                                }
                            ]
                        }
                                
                        """)
        );
        var folders = client.folderAsync("a/prefix", Instant.ofEpochSecond(1234), 100)
                .toCompletableFuture().get();
        assertThat(folders.entries).containsExactly(
                CatalogClient.Identifier.of("felles", 1610617128787L),
                CatalogClient.Identifier.of("kilde", 1604071017000L),
                CatalogClient.Identifier.of("produkt", 1608128434194L)
        );

        var request = server.takeRequest();
        assertThat(request.getPath()).isEqualTo("/folder?prefix=a%2Fprefix&limit=100&version=1970-01-01T00%3A20%3A34Z");
    }

    @Test
    void testListDataset() throws InterruptedException, ExecutionException {

        server.enqueue(new MockResponse()
                .setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
                .setResponseCode(200)
                .setBody("""
                        {
                            "entries": [
                                {
                                    "path": "/skatt/person/inputdata",
                                    "timestamp": "1583493564625"
                                },
                                {
                                    "path": "/skatt/person/rawdata-2019",
                                    "timestamp": "1582719098762"
                                }
                            ]
                        }
                        """)
        );
        var datasets = client.datasetAsync("a/prefix", Instant.ofEpochSecond(1234), 100)
                .toCompletableFuture().get();
        assertThat(datasets.entries).containsExactly(
                CatalogClient.Identifier.of("/skatt/person/inputdata", 1583493564625L),
                CatalogClient.Identifier.of("/skatt/person/rawdata-2019", 1582719098762L)
        );

        var request = server.takeRequest();
        assertThat(request.getPath()).isEqualTo("/dataset?prefix=a%2Fprefix&limit=100&version=1970-01-01T00%3A20%3A34Z");
    }

    @Test
    void testPrefixNotFound() throws ExecutionException, InterruptedException {
        server.enqueue(new MockResponse()
                .setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
                .setResponseCode(404)
        );
        var folder = client.datasetAsync("/prefix", Instant.ofEpochSecond(1234), 100)
                .toCompletableFuture().get();
        assertThat(folder).isNotNull();
    }
}