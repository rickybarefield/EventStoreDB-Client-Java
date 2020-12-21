package com.eventstore.dbclient;

import org.junit.Assert;
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

    private static final String PROJECTION_NAME = "counter";
    private static final String COUNT_EVENTS_PROJECTION_JS = "count-events-projection.js";
    private static final int EXPECTED_EVENT_COUNT = 2000;

    private static String COUNT_EVENTS_PROJECTION;

    @BeforeClass
    public static void loadProjectionJs() throws IOException {

        try(BufferedReader reader = new BufferedReader(new InputStreamReader(CreateProjectionTests.class.getClassLoader()
                .getResourceAsStream(COUNT_EVENTS_PROJECTION_JS)))) {

            COUNT_EVENTS_PROJECTION = reader.lines().collect(Collectors.joining("\n"));
        }
    }

    @Test
    public void testCreateAndGetContinuousProjection() throws ExecutionException, InterruptedException {

        createCountingProjection();

        CountResult result = getResultOfCountingProjection();

        assertCountingProjectionResultAsExpected(result);
    }


    private void createCountingProjection() throws InterruptedException, ExecutionException {

        server.getProjectionManagementAPI()
                .createContinuous(PROJECTION_NAME, COUNT_EVENTS_PROJECTION, false)
                .execute()
                .get();
    }

    private CountResult getResultOfCountingProjection() throws InterruptedException, ExecutionException {

        return server.getProjectionManagementAPI()
                .getResult(PROJECTION_NAME, CountResult.class)
                .execute()
                .get();
    }

    private void assertCountingProjectionResultAsExpected(final CountResult result) {

        Assert.assertNotNull(result);
        //The projection may not have completed so may not yet equal EXPECTED_EVENT_COUNT
        //that's okay we're not testing the server, just that the projection has been
        //created correctly and is running
        Assert.assertTrue(result.getCount() > 0);
        Assert.assertTrue(result.getCount() <= EXPECTED_EVENT_COUNT);
    }

    public static class CountResult {

        private int count;

        public int getCount() {
            return count;
        }

        public void setCount(final int count) {
            this.count = count;
        }
    }
}
