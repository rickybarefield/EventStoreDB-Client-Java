package com.eventstore.dbclient;

import com.eventstore.dbclient.proto.projections.Projectionmanagement;
import com.eventstore.dbclient.proto.projections.ProjectionsGrpc;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class GetProjectionResult<TResult> {

    private final GrpcClient client;
    private final String projectionName;
    private Class<TResult> resultType;

    private final ConnectionMetadata metadata;

    public GetProjectionResult(final GrpcClient client, final UserCredentials credentials,
                               final String projectionName, Class<TResult> resultType) {

        this.client = client;
        this.projectionName = projectionName;
        this.resultType = resultType;

        this.metadata = new ConnectionMetadata();

        if (credentials != null) {
            this.metadata.authenticated(credentials);
        }
    }

    public GetProjectionResult authenticated(UserCredentials credentials) {
        this.metadata.authenticated(credentials);
        return this;
    }

    public CompletableFuture<TResult> execute() {

        return this.client.run(channel -> {

            Projectionmanagement.ResultReq.Options.Builder optionsBuilder =
                    Projectionmanagement.ResultReq.Options.newBuilder()
                            .setName(projectionName);


            Projectionmanagement.ResultReq request = Projectionmanagement.ResultReq.newBuilder()
                    .setOptions(optionsBuilder)
                    .build();

            Metadata headers = this.metadata.build();

            ProjectionsGrpc.ProjectionsStub client = MetadataUtils.attachHeaders(ProjectionsGrpc.newStub(channel), headers);

            CompletableFuture<TResult> result = new CompletableFuture<>();

            Function<Projectionmanagement.ResultResp, TResult> converter = source -> {

                try {
                    String json = JsonFormat.printer().print(source.getResult());
                    return new JsonMapper().readValue(json, resultType);
                }
                catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                catch (JsonMappingException e) {
                    throw new RuntimeException(e);
                }
                catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            };

            client.result(request, GrpcUtils.convertSingleResponse(result, converter));

            return result;

        });
    }
}
