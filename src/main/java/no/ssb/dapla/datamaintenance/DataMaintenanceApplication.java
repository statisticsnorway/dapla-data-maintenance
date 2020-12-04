package no.ssb.dapla.datamaintenance;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.helidon.config.Config;
import io.helidon.microprofile.server.Server;
import no.ssb.dapla.datamaintenance.service.DataMaintenanceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DataMaintenanceApplication {

    private static final Logger LOG = LoggerFactory.getLogger(DataMaintenanceService.class);
    public static final ObjectMapper mapper = new ObjectMapper();

    public DataMaintenanceApplication() {
    }

    public static void main(String[] args) {
        Server server = startServer();
        LOG.info("Server is started: {}:{}", server.host(), server.port());
    }

    private static Server startServer() {
        return Server.builder()
                .config(buildConfig())
                .build()
                .start();
    }

    private static Config buildConfig() {
        return Config.builder()
                .metaConfig()
                .build();
    }

}
