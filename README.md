# EventStoreDB Client SDK for Java

This repository contains an [EventStoreDB][es] Client SDK written in Java for use with languages on the JVM. It is
compatible with Java 8 and above.

*Note: This client is currently under active development and further API changes are expected. Feedback is very welcome.*

## Access to binaries
EventStore Ltd publishes GA (general availability) versions to [Maven Central].

## Developing

The SDK is built using [`Gradle`][gradle]. Integration tests run against a server using Docker, with the [EventStoreDB gRPC
Client Test Container][container].

## EventStoreDB Server Compatibility

This client is compatible with version `20.6.1` upwards.

Server setup instructions can be found in the [docs], follow the docker setup for the simplest configuration.

## Example

The following snippet showcases a simple example where we form a connection, then write and read events from the server.

```java
class AccountCreated {
    private UUID id;
    private String login;

    public UUID getId() {
        return id;
    }

    public String getLogin() {
        return login;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public void setLogin(String login) {
        this.login = login;
    }
}
```
```java
import com.eventstore.dbclient.Client;
import com.eventstore.dbclient.ClientSettings;
import com.eventstore.dbclient.ConnectionString;
import com.eventstore.dbclient.GrpcClient;
import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.WriteResult;
import com.eventstore.dbclient.ReadResult;

public class Main {
    public static void main(String args[]) {
        ClientSettings setts = ConnectionString.parseOrThrow("esdb://localhost:2113");
        Client client = Client.create(setts);                        

        AccountCreated createdEvent = new AccountCreated();

        createdEvent.setId(UUID.randomUUID());
        createdEvent.setLogin("ouros");

        EventData event = EventData
                .builderAsJson("account-created", createdEvent)
                .build();

        WriteResult writeResult = client.streams()
                .appendStream("accounts")
                .addEvent(event)
                .execute()
                .get();

        ResolvedEvent resolvedEvent = client.streams()
                .readStream("accounts")
                .fromStart()
                .execute(1)
                .get()
                .getEvents()
                .get(0);

        AccountCreated writtenEvent = resolvedEvent.getOriginalEvent()
                .getEventDataAs(AccountCreated.class);
    
        // Doing something productive...
    }
}
```

## Projections

This client currently supports creating and getting the result of a continuous projection.

Create a projection:
```java
client.projections()
    .createContinuous(PROJECTION_NAME, PROJECTION_JS, false)
    .execute()
    .get();
```

Define a class in which to deserialize the result:
```java
public class CountResult {

    private int count;

    public int getCount() {
        return count;
    }

    public void setCount(final int count) {
        this.count = count;
    }
}
```

Get the result:
```java
CountResult result = client.projections()
    .getResult(PROJECTION_NAME, CountResult.class)
    .execute()
    .get();
```

For further details please see [the projection management tests](src/test/java/com/eventstore/dbclient/ProjectionManagementTests.java).

## Support

Information on support can be found on our website: [Event Store Support][support]

## Documentation

Documentation for EventStoreDB can be found in the [docs].

Bear in mind that this client is not yet properly documented. We are working hard on a new version of the documentation.

## Community

We have a community discussion space at [Event Store Discuss][discuss].

## Contributing

All contributions to the SDK are made via GitHub Pull Requests, and must be licensed under the Apache 2.0 license. Please
review our [Contributing Guide][contributing] and [Code of Conduct][code-of-conduct] for more information.

[es]: https://eventstore.com
[gradle]: https://gradle.org
[container]: https://github.com/EventStore/EventStore-Client-gRPC-TestData
[contributing]: https://github.com/EventStore/EventStoreDB-Client-Java/tree/master/CONTRIBUTING.md
[code-of-conduct]: https://github.com/EventStore/EventStoreDB-Client-Java/tree/master/CODE-OF-CONDUCT.md
[support]: https://eventstore.com/support/
[docs]: https://developers.eventstore.com/server/20.6/server/installation/
[discuss]: https://discuss.eventstore.com/
[Maven Central]: https://search.maven.org/artifact/com.eventstore/db-client-java
