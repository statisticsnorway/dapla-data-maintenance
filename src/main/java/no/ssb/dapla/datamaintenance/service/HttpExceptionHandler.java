package no.ssb.dapla.datamaintenance.service;

import io.helidon.webserver.HttpException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class HttpExceptionHandler implements ExceptionMapper<HttpException> {
    @Override
    public Response toResponse(HttpException exception) {
        return Response.status(exception.status().code())
                .type(MediaType.TEXT_PLAIN_TYPE)
                .entity(exception.getMessage())
                .build();
    }
}
