package no.ssb.dapla.datamaintenance.catalog;

import org.glassfish.jersey.internal.LocalizationMessages;
import org.glassfish.jersey.internal.inject.ExtractorException;

import javax.ws.rs.ext.ParamConverter;
import javax.ws.rs.ext.ParamConverterProvider;
import javax.ws.rs.ext.Provider;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.time.Instant;
import java.time.format.DateTimeParseException;

@Provider
public class InstantConverterProvider implements ParamConverterProvider {
    @Override
    public <T> ParamConverter<T> getConverter(Class<T> rawType, Type genericType, Annotation[] annotations) {
        return (rawType != Instant.class) ? null : new ParamConverter<T>() {

            @Override
            public T fromString(final String value) {
                if (value == null) {
                    throw new IllegalArgumentException(LocalizationMessages.METHOD_PARAMETER_CANNOT_BE_NULL("value"));
                }
                try {
                    return rawType.cast(Instant.parse(value));
                } catch (final DateTimeParseException ex) {
                    throw new ExtractorException(ex);
                }
            }

            @Override
            public String toString(final T value) throws IllegalArgumentException {
                if (value == null) {
                    throw new IllegalArgumentException(LocalizationMessages.METHOD_PARAMETER_CANNOT_BE_NULL("value"));
                }
                return value.toString();
            }
        };
    }
}