package no.ssb.dapla.datamaintenance.config;

import io.helidon.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.Initialized;
import javax.enterprise.event.Observes;
import javax.inject.Inject;

@ApplicationScoped
public class DataMaintenanceConfigProvider {

    private static final Logger LOG = LoggerFactory.getLogger(DataMaintenanceConfigProvider.class);
    
    @Inject
    Config config;

    public void onStartup(@Observes @Initialized(ApplicationScoped.class) Object init) {
    }

    public Config getConfig() {
        return config;
    }
}
