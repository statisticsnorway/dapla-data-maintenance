package no.ssb.dapla.datamaintenance;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.helidon.microprofile.config.ConfigCdiExtension;
import io.helidon.microprofile.tests.junit5.AddBean;
import io.helidon.microprofile.tests.junit5.AddExtension;
import io.helidon.microprofile.tests.junit5.HelidonTest;
import no.ssb.dapla.datamaintenance.model.CatalogItem;
import no.ssb.dapla.datamaintenance.model.DatasetListElement;
import no.ssb.dapla.datamaintenance.service.DataMaintenanceService;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@HelidonTest
@AddBean(DataMaintenanceApplication.class)
@AddBean(MockCatalogClient.class)
@AddExtension(ConfigCdiExtension.class)
class DataMaintenanceApplicationTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private static final ClassLoader classloader = Thread.currentThread().getContextClassLoader();

    @Inject
    private DataMaintenanceService service; // TODO does not work with @RequestScoped

    @Test
    public void testList() { // TODO use MockCatalogClient
        List<DatasetListElement> list = service.list("/felles");
        assertThat(list.size()).isEqualTo(12);
        assertThat(list.get(0).getPath()).isEqualTo("/felles/demo/dapla-oktober/eiendom_alle_felter");
    }

    @Test
    public void testSerializationAndDeserialization() throws Exception {

        // Test deserialization of response from the Catalog service
        String jsonString = new String(classloader.getResourceAsStream("listResponse.json").readAllBytes(),
                StandardCharsets.UTF_8);
        List<CatalogItem> cataogItems = CatalogItem.convertJSON(jsonString);
        assertThat(cataogItems.size()).isEqualTo(12);
        assertThat(cataogItems.get(0).getId().getPath()).isEqualTo("/felles/demo/dapla-oktober/eiendom_alle_felter");

        // Test serialization
        String serialized = mapper.writeValueAsString(cataogItems);
        //Add catalogs field
        serialized = "{\"catalogs\":" + serialized + "}";
        assertThat(serialized).isEqualToIgnoringWhitespace(jsonString);
    }
}