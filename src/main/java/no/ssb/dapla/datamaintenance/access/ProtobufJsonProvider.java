package no.ssb.dapla.datamaintenance.access;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;

import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

@Provider
@Consumes({"application/json", "text/json"})
@Produces({"application/json", "text/json"})
public class ProtobufJsonProvider implements MessageBodyWriter<Message>, MessageBodyReader<Message> {

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return doesSupport(type, mediaType);
    }

    private boolean doesSupport(Class<?> type, MediaType mediaType) {
        return MediaType.APPLICATION_JSON_TYPE.equals(mediaType)
                && MessageOrBuilder.class.isAssignableFrom(type);
    }

    @Override
    public void writeTo(Message message, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        write(entityStream, message);
    }

    @Override
    public boolean isReadable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return doesSupport(type, mediaType);
    }

    @Override
    public Message readFrom(Class<Message> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String, String> httpHeaders, InputStream entityStream) throws IOException, WebApplicationException {
        return read(entityStream, type);
    }

    public static <T extends Message> void write(OutputStream output, MessageOrBuilder message) throws IOException {
        try (var writer = new PrintStream(output)) {
            JsonFormat.printer().appendTo(message, writer);
        }
    }

    public static String writeAsString(MessageOrBuilder message) throws IOException {
        try (var stream = new ByteArrayOutputStream()) {
            write(stream, message);
            return stream.toString();
        }
    }

    public static <T extends Message> T read(InputStream input, Class<T> type) throws IOException {
        try (var reader = new InputStreamReader(input)) {
            Method newBuilderMethod = type.getMethod("newBuilder", (Class[])null);
            Message.Builder builder = (Message.Builder)newBuilderMethod.invoke((Object)null);
            JsonFormat.parser().merge(reader, builder);
            return (T) builder.build();
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IOException(e);
        }
    }

}
