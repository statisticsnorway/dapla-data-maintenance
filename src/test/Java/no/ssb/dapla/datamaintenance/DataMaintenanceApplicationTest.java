package no.ssb.dapla.datamaintenance;

import io.helidon.microprofile.config.ConfigCdiExtension;
import io.helidon.microprofile.tests.junit5.AddBean;
import io.helidon.microprofile.tests.junit5.AddExtension;
import io.helidon.microprofile.tests.junit5.HelidonTest;
import no.ssb.dapla.datamaintenance.service.DataMaintenanceService;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@HelidonTest
@AddBean(DataMaintenanceApplication.class)
@AddExtension(ConfigCdiExtension.class)
class DataMaintenanceApplicationTest {

    @Inject
    private DataMaintenanceService service; // TODO does not work with @RequestScoped

    @Test
    public void testEndpoint() {
        assertThat(service.test().get("message").toString()).isEqualTo("\"Server is up and running\"");
    }
}