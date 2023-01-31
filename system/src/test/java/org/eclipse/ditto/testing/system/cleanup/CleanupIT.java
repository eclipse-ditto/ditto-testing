/*
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */
package org.eclipse.ditto.testing.system.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.testing.common.TestConstants.API_V_2;
import static org.hamcrest.Matchers.containsString;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.bson.BsonDocument;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonCollectors;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.common.matcher.PostMatcher;
import org.eclipse.ditto.testing.common.matcher.StatusCodeSuccessfulMatcher;
import org.junit.After;
import org.junit.Test;

import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * Tests background deletion of events and snapshots.
 * This should be the only test in a fresh environment.
 */
@RunIf(DockerEnvironment.class)
public final class CleanupIT extends IntegrationTest {

    /**
     * How many extra events to generate for each thing, policy or connection.
     * Should be at least as large as the maximum snapshot threshold of things-, policies- and connectivity-service.
     */
    private static final int EXTRA_EVENTS = 10;
    private static final Duration WAIT_FOR_MONGO_WRITES = Duration.ofSeconds(20L);
    private static final Duration WAIT_FOR_CLEANUP = Duration.ofMinutes(3L);
    private static final String CONNECTION_PREFIX = "connection:";

    @After
    public void disableCleanUp() {
        stopCleanup();
    }

    @Test(timeout = 600_000)
    public void staleEventsAndSnapshotsAreDeleted() throws Exception {
        // stop cleanup to avoid deletion of events during creation
        stopCleanup();
        try (final MongoClient mongoClient = createMongoClient()) {
            // create 5 things with snapshots whose default policies have snapshots
            final List<String> thingIds = IntStream.range(0, 5)
                    .mapToObj(i -> createThing())
                    .collect(Collectors.toList());
            thingIds.forEach(thingId -> {
                repeat(EXTRA_EVENTS, updateAttribute(thingId));
                repeat(EXTRA_EVENTS, updatePolicy(thingId));
            });

            // create 1 closed connection and 1 open connection
            final String closedConnectionId = createConnectionWithAdditionalEvents(false, EXTRA_EVENTS);
            final String openConnectionId = createConnectionWithAdditionalEvents(true, EXTRA_EVENTS);

            // check the persisted event counts are equal to the expectation
            final int expectedThingAndPolicyEvents = thingIds.size() * (EXTRA_EVENTS + 1);
            final int expectedConnectionEvents = 2 * (EXTRA_EVENTS + 2);

            // wait for MongoDB writes to be readable
            LOGGER.info("Waiting <{}> until writes on MongoDB are readable.", WAIT_FOR_MONGO_WRITES);
            TimeUnit.SECONDS.sleep(WAIT_FOR_MONGO_WRITES.getSeconds());
            LOGGER.info("Asserting the correct number of events and snapshots.");

            assertThat(countThingEvents(mongoClient, thingIds))
                    .as("thing events")
                    .isEqualTo(expectedThingAndPolicyEvents);
            assertThat(countPolicyEvents(mongoClient, thingIds))
                    .as("policy events")
                    .isEqualTo(expectedThingAndPolicyEvents);
            assertThat(countConnectionEvents(mongoClient, List.of(closedConnectionId, openConnectionId), true))
                    .as("connection events")
                    .isEqualTo(expectedConnectionEvents);
            // check that snapshots are made
            for (final String thingId : thingIds) {
                assertThat(countThingSnaps(mongoClient, List.of(thingId)))
                        .as("thing snapshot")
                        .isNotZero();
                assertThat(countPolicySnaps(mongoClient, List.of(thingId)))
                        .as("policy snapshot")
                        .isNotZero();
            }
            assertThat(countConnectionSnaps(mongoClient, List.of(closedConnectionId)))
                    .as("connection snapshot for closed connection")
                    .isNotZero();
            assertThat(countConnectionSnaps(mongoClient, List.of(openConnectionId)))
                    .as("connection snapshot for open connection")
                    .isNotZero();

            LOGGER.info("Starting cleanup.");
            // run cleanup
            startCleanup(thingIds, List.of(openConnectionId, closedConnectionId));

            LOGGER.info("Sleeping for <{}> before validating that events and snapshots have been cleaned up" +
                    " as expected,", WAIT_FOR_CLEANUP);
            // sleep for a long time and wait for cleanup
            TimeUnit.SECONDS.sleep(WAIT_FOR_CLEANUP.getSeconds());
            LOGGER.info("Validating that events and snapshots have been cleaned up as expected.");

            // there is always 1 journal entry retained on cleanup:
            final int expectedJournalEntriesAfterCleanup = thingIds.size();

            // check that things and closed connections have no events and open connection has 1 event
            assertThat(countThingEvents(mongoClient, thingIds))
                    .as("thing events")
                    .isEqualTo(expectedJournalEntriesAfterCleanup);
            assertThat(countPolicyEvents(mongoClient, thingIds))
                    .as("policy events")
                    .isEqualTo(expectedJournalEntriesAfterCleanup);
            // even closed connections have 1 journal entry retained on cleanup:
            assertThat(countConnectionEvents(mongoClient, List.of(closedConnectionId), false))
                    .as("closed connection events")
                    .isEqualTo(1);
            assertThat(countConnectionEvents(mongoClient, List.of(openConnectionId), true))
                    .as("open connection events")
                    .isEqualTo(1);

            // check that all but 1 snapshots are cleaned up
            for (final String thingId : thingIds) {
                assertThat(countThingSnaps(mongoClient, List.of(thingId)))
                        .as("thing snapshots")
                        .isEqualTo(1);
                assertThat(countPolicySnaps(mongoClient, List.of(thingId)))
                        .as("thing snapshots")
                        .isEqualTo(1);
            }
            assertThat(countConnectionSnaps(mongoClient, List.of(closedConnectionId)))
                    .as("connection snapshot for closed connection")
                    .isEqualTo(1);
            assertThat(countConnectionSnaps(mongoClient, List.of(openConnectionId)))
                    .as("connection snapshot for closed connection")
                    .isEqualTo(1);
        }
    }

    private void startCleanup(final List<String> thingIds, final List<String> connectionIds) {
        final String thingAndPolicyLowerBound = getLowerBound(thingIds);
        setCleanupConfig("things", true, 1L, "thing:" + thingAndPolicyLowerBound);
        setCleanupConfig("policies", true, 1L, "policy:" + thingAndPolicyLowerBound);
        setCleanupConfig("connectivity", true, 1L, "connection:" + getLowerBound(connectionIds));
    }

    private void stopCleanup() {
        setCleanupConfig("things", false, 86400L, "");
        setCleanupConfig("policies", false, 86400L, "");
        setCleanupConfig("connectivity", false, 86400L, "");
    }

    private void setCleanupConfig(final String service, final boolean enabled, final long quietPeriod,
            final String lastPid) {
        final JsonObject modifyConfig = JsonObject.newBuilder()
                .set("type", "common.commands:modifyConfig")
                .set(JsonPointer.of("config/enabled"), enabled)
                .set(JsonPointer.of("config/quiet-period"), quietPeriod + "s")
                .set(JsonPointer.of("config/interval"), "1s")
                .set(JsonPointer.of("config/last-pid"), lastPid)
                .build();

        postPiggy(service, modifyConfig)
                .expectingBody(containsString("common.responses:modifyConfig"))
                .fire();
    }

    private PostMatcher postPiggy(final String service, final JsonObject payload) {
        return serviceEnv.postPiggy(service, payload, "/user/" + service + "Root/persistenceCleanup");
    }

    private static String getLowerBound(final List<String> ids) {
        final String smallest = ids.stream().sorted().findFirst().orElse("");
        if (smallest.isEmpty()) {
            return smallest;
        } else {
            return smallest.substring(0, smallest.length() - 1);
        }
    }

    private static void repeat(final int repetitions, final Consumer<Integer> consumer) {
        for (int i = 0; i < repetitions; ++i) {
            consumer.accept(i);
        }
    }

    private static String createThing() {
        return parseIdFromResponse(postThing(API_V_2, JsonObject.empty())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());
    }

    private static Consumer<Integer> updateAttribute(final String thingId) {
        return randomInt ->
                putAttribute(API_V_2, thingId, "x" + randomInt, String.valueOf(randomInt))
                        .expectingStatusCode(StatusCodeSuccessfulMatcher.getInstance())
                        .fire();
    }

    private static Consumer<Integer> updatePolicy(final String policyId) {
        return randomInt ->
                putPolicyEntry(policyId, PoliciesModelFactory.newPolicyEntry("dummy" + randomInt,
                        PoliciesModelFactory.newSubjects(
                                Subject.newInstance(SubjectIssuer.INTEGRATION, "dummy" + randomInt)),
                        PoliciesModelFactory.newResources(PoliciesModelFactory.newResource("thing", "/",
                                EffectedPermissions.newInstance(Collections.singletonList("nothing"), null)
                        ))))
                        .expectingHttpStatus(HttpStatus.CREATED)
                        .fire();
    }

    private static String createConnectionWithAdditionalEvents(final boolean open, final int amountOfAdditionalEvents) {
        final String username = serviceEnv.getDefaultAuthUsername();
        final String secret = serviceEnv.getDefaultTestingContext()
                .getSolution()
                .getSecret();
        final JsonObject closedConnection = TestConstants.Connections.buildConnection();
        final JsonObject finalConnection = closedConnection.toBuilder()
                .set(Connection.JsonFields.CONNECTION_STATUS, open ? "open" : "closed")
                .build();

        final String connectionId = parseIdFromResponse(connectionsClient()
                .postConnection(closedConnection)
                .withHeader(HttpHeader.TIMEOUT, 60)
                .withBasicAuth(username, secret, true)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        repeat(amountOfAdditionalEvents, i -> connectionsClient()
                .putConnection(connectionId, closedConnection)
                .withBasicAuth(username, secret, true)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire());

        connectionsClient()
                .putConnection(connectionId, finalConnection)
                .withBasicAuth(username, secret, true)
                .expectingHttpStatus(open ? HttpStatus.GATEWAY_TIMEOUT : HttpStatus.NO_CONTENT)
                .fire();

        return connectionId;
    }

    private static MongoClient createMongoClient() {
        return MongoClients.create(TEST_CONFIG.getMongoDBUri());
    }

    private static long countThingEvents(final MongoClient mongoClient, final List<? extends CharSequence> thingIds) {
        return countDocuments(mongoClient, thingIds, "things", "things_journal", "thing:",
                false);
    }

    private static long countThingSnaps(final MongoClient mongoClient, final List<? extends CharSequence> thingIds) {
        return countDocuments(mongoClient, thingIds, "things", "things_snaps", "thing:",
                false);
    }

    private static long countPolicyEvents(final MongoClient mongoClient, final List<? extends CharSequence> policyIds) {
        return countDocuments(mongoClient, policyIds, "policies", "policies_journal", "policy:",
                false);
    }

    private static long countPolicySnaps(final MongoClient mongoClient, final List<? extends CharSequence> policyIds) {
        return countDocuments(mongoClient, policyIds, "policies", "policies_snaps", "policy:",
                false);
    }

    private static long countConnectionEvents(final MongoClient mongoClient,
            final List<? extends CharSequence> policyIds, final boolean filterConnectivityEvents) {
        return countDocuments(mongoClient, policyIds, "connectivity", "connection_journal",
                CONNECTION_PREFIX, filterConnectivityEvents);
    }

    private static long countConnectionSnaps(final MongoClient mongoClient,
            final List<? extends CharSequence> policyIds) {
        return countDocuments(mongoClient, policyIds, "connectivity", "connection_snaps",
                CONNECTION_PREFIX, false);
    }

    private static long countDocuments(final MongoClient mongoClient,
            final List<? extends CharSequence> ids,
            final String database, final String collection,
            final String prefix,
            final boolean filterConnectivityEvents) {

        final JsonArray idsJson = ids.stream()
                .map(thingId -> prefix + thingId)
                .map(JsonValue::of)
                .collect(JsonCollectors.valuesToArray());

        final BsonDocument bsonDocument;
        if (CONNECTION_PREFIX.equals(prefix) && filterConnectivityEvents) {
            // filter out empty-events
            bsonDocument = BsonDocument.parse(String.format("{$and:[{\"pid\":{\"$in\":%s}}," +
                    "{'events.p.type': { $regex: \"connectivity.events:\"}}]}", idsJson));
        } else {
            bsonDocument = BsonDocument.parse(String.format("{\"pid\":{\"$in\":%s}}", idsJson));
        }

        return mongoClient.getDatabase(database)
                .getCollection(collection)
                .withReadPreference(ReadPreference.primary())
                .countDocuments(bsonDocument);
    }

}
