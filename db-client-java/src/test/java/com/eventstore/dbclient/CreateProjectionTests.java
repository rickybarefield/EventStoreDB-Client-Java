package com.eventstore.dbclient;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import testcontainers.module.EventStoreTestDBContainer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class CreateProjectionTests {

    @Rule
    public final EventStoreTestDBContainer server = new EventStoreTestDBContainer(false);

    private static String COUNT_EVENTS_PROJECTION;

    @BeforeClass
    public static void loadProjectionJs() throws IOException {

        try(BufferedReader reader = new BufferedReader(new InputStreamReader(CreateProjectionTests.class.getClassLoader()
                .getResourceAsStream("count-events-projection.js")))) {

            COUNT_EVENTS_PROJECTION = reader.lines().collect(Collectors.joining("\n"));
        }
    }

    @Test
    public void testCreateGetAddToGetAndDeleteContinuousProjection() throws ExecutionException, InterruptedException {

        Projections projections = server.getProjectionManagementAPI();
        projections
                .createContinuous("projection", COUNT_EVENTS_PROJECTION, false)
                .execute()
                .get();

        //get projection and check result is 2000

        //delete projection

        //get projection and check error message
    }
}
