package com.eventstore.dbclient;

public class Projections {

    private GrpcClient client;
    private UserCredentials credentials;

    public Projections(final GrpcClient client, final UserCredentials credentials) {

        this.client = client;
        this.credentials = credentials;
    }

    public CreateProjection createContinuous(final String projectionName, final String query, final boolean trackEmittedStreams) {

        return CreateProjection.forContinuous(this.client, this.credentials, projectionName, query, trackEmittedStreams);
    }

    public <TResult> GetProjectionResult<TResult> getResult(final String projectionName, Class<TResult> type) {

        return new GetProjectionResult<>(this.client, this.credentials, projectionName, type);
    }
}
