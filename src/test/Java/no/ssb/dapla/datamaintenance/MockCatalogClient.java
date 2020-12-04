package no.ssb.dapla.datamaintenance;

import no.ssb.dapla.datamaintenance.catalog.CatalogClient;

import javax.annotation.Priority;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Alternative;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Alternative
@Priority(1)
@ApplicationScoped
public class MockCatalogClient implements CatalogClient { // TODO make this work

    private static final ClassLoader classloader = Thread.currentThread().getContextClassLoader();

    @Override
    public String list() {
        try {
            return new String(classloader.getResourceAsStream("listResponse.json").readAllBytes(),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
