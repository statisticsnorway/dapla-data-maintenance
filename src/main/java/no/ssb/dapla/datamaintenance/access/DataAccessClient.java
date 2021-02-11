package no.ssb.dapla.datamaintenance.access;

import no.ssb.dapla.data.access.protobuf.DeleteLocationRequest;
import no.ssb.dapla.data.access.protobuf.DeleteLocationResponse;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;

import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import java.util.concurrent.CompletionStage;

@RegisterProvider(ProtobufJsonProvider.class)
public interface DataAccessClient {

    @Path("/rpc/DataAccessService/deleteLocation")
    @POST
    CompletionStage<DeleteLocationResponse> deleteLocation(DeleteLocationRequest request,
                                                           @HeaderParam("Authorization") String authorization);

}
