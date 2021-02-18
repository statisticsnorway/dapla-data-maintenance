package no.ssb.dapla.datamaintenance;

import io.helidon.common.http.Http;
import io.helidon.webserver.HttpException;

import javax.ws.rs.WebApplicationException;
import java.util.function.Consumer;

public class ExceptionConverter implements Consumer<Throwable> {

    private final String prefix;

    public ExceptionConverter(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public void accept(Throwable throwable) {
        Http.ResponseStatus status = Http.Status.SERVICE_UNAVAILABLE_503;
        StringBuilder message = new StringBuilder(prefix);
        message.append(" ").append(throwable.getMessage());
        if (throwable instanceof WebApplicationException) {
            status = Http.ResponseStatus.create(
                    ((WebApplicationException) throwable).getResponse().getStatus()
            );
            try {
                String entity = ((WebApplicationException) throwable).getResponse().readEntity(String.class);
                message.append("\n").append(entity);
            } catch (Exception e) {
                // Ignore.
            }
        }
        throw new HttpException(message.toString(), status, throwable);
    }
}
