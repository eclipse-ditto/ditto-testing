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
package org.eclipse.ditto.testing.system.connectivity.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.testing.common.TestConstants.API_V_2;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.auth.AuthorizationContext;
import org.eclipse.ditto.base.model.auth.AuthorizationSubject;
import org.eclipse.ditto.base.model.auth.DittoAuthorizationContextType;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.LogEntry;
import org.eclipse.ditto.connectivity.model.Source;
import org.eclipse.ditto.connectivity.model.SshTunnel;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.connectivity.model.Topic;
import org.eclipse.ditto.connectivity.model.UserPasswordCredentials;
import org.eclipse.ditto.connectivity.model.signals.commands.query.RetrieveConnectionLogsResponse;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonCollectors;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.policies.model.SubjectType;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.ThingsSubjectIssuer;
import org.eclipse.ditto.testing.system.connectivity.ConnectionModelFactory;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityFactory;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityTestConfig;
import org.eclipse.ditto.testing.system.connectivity.kafka.KafkaConnectivityWorker;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.restassured.response.Response;

public final class RestConnectionsIT extends IntegrationTest {

    private static final int TIMEOUT = 30;
    private static final ConnectivityTestConfig CONFIG = ConnectivityTestConfig.getInstance();

    private static final String CONNECTION_NAME_PREFIX = RestConnectionsIT.class.getSimpleName();
    private static final SshTunnel DISABLED_SSH_TUNNEL = ConnectivityModelFactory.newSshTunnel(false,
            UserPasswordCredentials.newInstance("dummy", "dummy"), "ssh://localhost:22");

    private static TestingContext testingContext;
    private static String integrationSubject;
    private static ThingId thingId;

    private static final String KAFKA_TEST_CLIENTID = RestConnectionsIT.class.getSimpleName();
    private static final String KAFKA_TEST_HOSTNAME = CONFIG.getKafkaHostname();
    private static final String KAFKA_TEST_USERNAME = CONFIG.getKafkaUsername();
    private static final String KAFKA_TEST_PASSWORD = CONFIG.getKafkaPassword();
    private static final int KAFKA_TEST_PORT = CONFIG.getKafkaPort();

    private static final String KAFKA_SERVICE_HOSTNAME = CONFIG.getKafkaHostname();
    private static final int KAFKA_SERVICE_PORT = CONFIG.getKafkaPort();
    private final ConnectivityFactory connectivityFactory;

    private ConnectionId defaultConnectionId;

    @BeforeClass
    public static void createSolution() throws InterruptedException, TimeoutException, ExecutionException {
        testingContext = serviceEnv.getDefaultTestingContext();

        integrationSubject = SubjectIssuer.INTEGRATION + ":test";

        thingId = ThingId.of(idGenerator(testingContext.getSolution().getDefaultNamespace()).withRandomName());
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();

        final Policy policy = Policy.newBuilder(PolicyId.of(thingId))
                .setSubjectsFor("Default", Subjects.newInstance(
                        Subject.newInstance(integrationSubject, SubjectType.GENERATED),
                        Subject.newInstance(ThingsSubjectIssuer.DITTO, testingContext.getSolution().getUsername())))
                .setGrantedPermissionsFor("Default", PoliciesResourceType.thingResource("/"), "READ", "WRITE")
                .setGrantedPermissionsFor("Default", PoliciesResourceType.policyResource("/"), "READ", "WRITE")
                .setGrantedPermissionsFor("Default", PoliciesResourceType.messageResource("/"), "READ", "WRITE")
                .build();

        putThingWithPolicy(API_V_2, thing, policy, JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getDefaultTestingContext())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        LOGGER.info("Preparing Kafka at {}:{}", KAFKA_TEST_HOSTNAME, KAFKA_TEST_PORT);
        KafkaConnectivityWorker.setupKafka(KAFKA_TEST_CLIENTID, KAFKA_TEST_HOSTNAME, KAFKA_TEST_PORT,
                KAFKA_TEST_USERNAME, KAFKA_TEST_PASSWORD,
                Collections.singleton(defaultTargetAddress(CONNECTION_NAME_PREFIX)), LOGGER);
    }

    @Before
    public void createDefaultConnection() throws InterruptedException, ExecutionException, TimeoutException {
        final Response response = connectivityFactory.setupSingleConnection(CONNECTION_NAME_PREFIX + "-" + UUID.randomUUID())
                .get(TIMEOUT, TimeUnit.SECONDS);
        final Connection connection = ConnectivityModelFactory.connectionFromJson(
                JsonFactory.newObject(response.getBody().asString()));
        defaultConnectionId = connection.getId();
    }

    @After
    public void deleteDefaultConnection() {
        connectionsClient().deleteConnection(defaultConnectionId)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    private static String getConnectionUri(final boolean tunnel, final boolean basicAuth) {
        // Tunneling is not implemented for Kafka
        return "tcp://" + KAFKA_TEST_USERNAME + ":" + KAFKA_TEST_PASSWORD +
                "@" + KAFKA_SERVICE_HOSTNAME + ":" + KAFKA_SERVICE_PORT;
    }

    private static Map<String, String> getSpecificConfig() {
        return Collections.singletonMap("bootstrapServers", KAFKA_SERVICE_HOSTNAME + ":" + KAFKA_SERVICE_PORT);
    }

    private static String defaultTargetAddress(final String suffix) {
        return "test-target-" + suffix;
    }

    public RestConnectionsIT() {
        final ConnectionModelFactory connectionModelFactory =
                new ConnectionModelFactory((username, suffix) -> integrationSubject);
        connectivityFactory = ConnectivityFactory.of("Rest",
                connectionModelFactory,
                ConnectionType.KAFKA,
                RestConnectionsIT::getConnectionUri,
                RestConnectionsIT::getSpecificConfig,
                RestConnectionsIT::defaultTargetAddress,
                connectionId -> null,
                connectionId -> null,
                () -> DISABLED_SSH_TUNNEL
        ).withSolutionSupplier(() -> testingContext.getSolution())
                .withAuthClient(testingContext.getOAuthClient());
    }

    @Test
    public void createConnection() {
        // WHEN
        final JsonObject connection = TestConstants.Connections.buildConnection();

        final String connectionId = parseIdFromResponse(connectionsClient()
                .postConnection(connection)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        connectionsClient().getConnection(connectionId)
                .withDevopsAuth()
                .expectingBody(contains(connection.toBuilder().set("id", connectionId).build()))
                .fire();
    }

    @Test
    public void retrieveConnection() {
        // WHEN
        connectionsClient()
                .getConnection(defaultConnectionId)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void createConnectionWithTwoTargetsHavingTheSameIssuedAck() {
        // WHEN
        final var username = testingContext.getSolution().getUsername();
        final AuthorizationContext authContext = AuthorizationContext.newInstance(
                DittoAuthorizationContextType.PRE_AUTHENTICATED_CONNECTION,
                AuthorizationSubject.newInstance("integration:" + username + ":" + TestingContext.DEFAULT_SCOPE));
        final JsonObject connection = TestConstants.Connections.buildConnection().toBuilder()
                .set(Connection.JsonFields.TARGETS, JsonArray.of(ConnectivityModelFactory.newTargetBuilder()
                                .address("amqp/target1")
                                .authorizationContext(authContext)
                                .topics(Topic.TWIN_EVENTS, Topic.LIVE_EVENTS)
                                .issuedAcknowledgementLabel(AcknowledgementLabel.of("{{connection:id}}:test"))
                                .build().toJson(),
                        ConnectivityModelFactory.newTargetBuilder()
                                .address("amqp/target2")
                                .authorizationContext(authContext)
                                .topics(Topic.TWIN_EVENTS, Topic.LIVE_EVENTS)
                                .issuedAcknowledgementLabel(AcknowledgementLabel.of("{{connection:id}}:test"))
                                .build().toJson()))
                .build();

        // THEN: connection creation is rejected as conflict
        connectionsClient()
                .postConnection(connection)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.CONFLICT)
                .fire();
    }

    @Test
    public void createConnectionWithTargetIssuedAckNotCorrectlyPrefixed() {
        // WHEN
        final var username = testingContext.getSolution().getUsername();
        final AuthorizationContext authContext = AuthorizationContext.newInstance(
                DittoAuthorizationContextType.PRE_AUTHENTICATED_CONNECTION,
                AuthorizationSubject.newInstance("integration:" + username + ":" + TestingContext.DEFAULT_SCOPE));
        final JsonObject connection = TestConstants.Connections.buildConnection().toBuilder()
                .set(Connection.JsonFields.TARGETS, JsonArray.of(ConnectivityModelFactory.newTargetBuilder()
                        .address("amqp/target1")
                        .authorizationContext(authContext)
                        .topics(Topic.TWIN_EVENTS, Topic.LIVE_EVENTS)
                        .issuedAcknowledgementLabel(AcknowledgementLabel.of("somethingWrong:test"))
                        .build().toJson()))
                .build();

        connectionsClient()
                .postConnection(connection)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("acknowledgement:label.invalid")
                .fire();
    }

    @Test
    public void createConnectionWithDeclaredAckNotCorrectlyPrefixed() {
        // WHEN
        final var username = testingContext.getSolution().getUsername();
        final AuthorizationContext authContext = AuthorizationContext.newInstance(
                DittoAuthorizationContextType.PRE_AUTHENTICATED_CONNECTION,
                AuthorizationSubject.newInstance("integration:" + username + ":" + TestingContext.DEFAULT_SCOPE));
        final JsonObject connection = TestConstants.Connections.buildConnection().toBuilder()
                .set(Connection.JsonFields.SOURCES,
                        JsonArray.of(ConnectivityModelFactory.newSourceBuilder()
                                .authorizationContext(authContext)
                                .address("amqp/source1")
                                .declaredAcknowledgementLabels(Set.of(AcknowledgementLabel.of("somethingWrong:test")))
                                .build().toJson()))
                .build();

        connectionsClient()
                .postConnection(connection)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("acknowledgement:label.invalid")
                .fire();
    }

    @Test
    public void createConnectionByPutWorks() {
        // WHEN
        final String connectionName = UUID.randomUUID().toString();
        final JsonObject connection = TestConstants.Connections.buildConnection("0", connectionName);

        connectionsClient()
                .putConnection(connectionName, connection)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.CREATED) // Creating via PUT is supported
                .fire();
    }

    @Test
    public void logsShouldBeEnabledAfterCreation() throws InterruptedException {
        assertLogsEnabledOnOpening();

        updateThing();

        TimeUnit.MILLISECONDS.sleep(500);
        final Collection<LogEntry> before = assertLogEntries();

        resetLogs();

        TimeUnit.MILLISECONDS.sleep(500);
        assertLessLogEntries(before);
    }

    @Test
    public void logsShouldBeEnabledAfterOpeningConnection() throws InterruptedException {
        closeConnection();

        assertLogsNotEnabled();

        openConnection();

        assertLogsEnabledOnOpening();

        updateThing();

        TimeUnit.MILLISECONDS.sleep(500);
        final Collection<LogEntry> before = assertLogEntries();

        resetLogs();

        TimeUnit.MILLISECONDS.sleep(500);
        assertLessLogEntries(before);
    }

    @Test
    public void createConnectionWithSshTunnel() {
        // WHEN
        final JsonObject connection = TestConstants.Connections.buildConnectionWithSshTunnel("sshTunnelConnection");

        final String sshTunnelJsonString = TestConstants.Connections.sshTunnelTemplate().toString();
        connectionsClient()
                .postConnection(connection)
                .withDevopsAuth()
                .expectingBody(satisfies(
                        jsonString -> assertThat(String.valueOf(jsonString)).contains(sshTunnelJsonString)))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
    }

    @Test
    public void cannotCreateConnectionWithConnectionAnnouncementsAndClientCountGreaterOne() {
        // WHEN
        final JsonObject connection = TestConstants.Connections.buildConnection();
        final JsonObject connectionWithTarget = connection
                .set(Connection.JsonFields.CLIENT_COUNT, 2)
                .set(Connection.JsonFields.TARGETS,
                        JsonArray.of(JsonObject.newBuilder()
                                .set(Target.JsonFields.ADDRESS, "telemetry/tenant")
                                .set(Target.JsonFields.TOPICS,
                                        JsonArray.of(JsonValue.of(Topic.CONNECTION_ANNOUNCEMENTS.toString())))
                                .set(Source.JsonFields.AUTHORIZATION_CONTEXT,
                                        JsonArray.of(JsonValue.of("integration:" +
                                                testingContext.getSolution().getUsername() + ":" + TestingContext.DEFAULT_SCOPE)))
                                .build()));

        connectionsClient()
                .postConnection(connectionWithTarget)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    @Test
    public void createConnectionWithUnknownPipelineFunctionInTargetTopicFilterFails() {
        // WHEN the target topic 'fn-filter' references an unknown pipeline function
        final JsonObject connection = connectionWithTargetTopics(
                "_/_/things/twin/events?fn-filter=fn:unknownfn('x')");

        // THEN connection creation is rejected as invalid connection configuration
        connectionsClient()
                .postConnection(connection)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("connectivity:connection.configuration.invalid")
                .fire();
    }

    @Test
    public void createConnectionWithMalformedRqlFilterParamAlongsidePipelineFilterParamFails() {
        // WHEN the RQL 'filter' param of a topic is malformed
        // (the 'fn-filter' param alongside it is valid - the RQL param must still be rejected)
        final JsonObject connection = connectionWithTargetTopics(
                "_/_/things/twin/events?filter=gt(attributes/x,)&fn-filter=fn:filter(header:x,'exists')");

        connectionsClient()
                .postConnection(connection)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("rql.expression.invalid")
                .fire();
    }

    @Test
    public void createConnectionWithPipelineExpressionInRqlFilterParamFailsPointingToFnFilter() {
        // WHEN a placeholder pipeline expression is put into the RQL-only 'filter' param instead of 'fn-filter'
        // THEN it is rejected as invalid connection configuration BEFORE reaching the RQL parser, and the error
        // description tells the user to move it into 'fn-filter'
        final JsonObject connection = connectionWithTargetTopics(
                "_/_/things/twin/events?filter=fn:filter(header:ditto-originator,'ne','integration:some:excluded')");

        connectionsClient()
                .postConnection(connection)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("connectivity:connection.configuration.invalid")
                .expectingBody(satisfies(jsonString -> assertThat(String.valueOf(jsonString))
                        .contains("'filter' only accepts an RQL expression")
                        .contains("fn-filter")))
                .fire();
    }

    @Test
    public void createConnectionWithNamelessLeadingPlaceholderInFnFilterFails() {
        // WHEN a placeholder-first 'fn-filter' starts with a placeholder prefix without a name
        // (passes the pipeline grammar but could never be evaluated at runtime)
        final JsonObject connection = connectionWithTargetTopics(
                "_/_/things/twin/events?fn-filter=header:|fn:filter('eq','x')");

        connectionsClient()
                .postConnection(connection)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("connectivity:connection.configuration.invalid")
                .fire();
    }

    @Test
    public void createConnectionWithLegacyCombinedFilterSyntaxFails() {
        // WHEN a single filter param uses the retired combined "<rql>|fn:..." syntax
        // THEN it is routed whole into the RQL parser (it does not start with "fn:") and fails loudly -
        // it must never be silently split or accepted
        final JsonObject connection = connectionWithTargetTopics(
                "_/_/things/twin/events?filter=gt(attributes/counter,42)|fn:filter(header:x,'exists')");

        connectionsClient()
                .postConnection(connection)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("rql.expression.invalid")
                .fire();
    }

    @Test
    public void createConnectionWithWhitespaceOnlyTargetTopicFilterFails() {
        // WHEN the target topic filter is whitespace-only
        // (%20%20 is url-decoded to two spaces by FilteredTopic parsing and must be rejected exactly as an
        // empty filter was rejected before target topic pipeline filters existed)
        final JsonObject connection = connectionWithTargetTopics(
                "_/_/things/twin/events?filter=%20%20");

        connectionsClient()
                .postConnection(connection)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("rql.expression.invalid")
                .fire();
    }

    @Test
    public void createConnectionWithValidPipelineTargetTopicFilters() {
        // WHEN a connection defines pure-pipeline, placeholder-first, chained-pipeline and RQL-plus-pipeline
        // target topic filters ('fn-filter' alone in both documented forms, a chained 'fn-filter', and
        // 'filter' + 'fn-filter' ANDed) - including an unknown rqlFunction NAME ('nope'), which is accepted at
        // creation time (documented behavior; it simply never matches at runtime)
        final JsonObject connection = connectionWithTargetTopics(
                "_/_/things/twin/events?fn-filter=fn:filter(header:ditto-originator,'ne','integration:some:excluded')",
                "_/_/things/twin/events?fn-filter=header:ditto-originator|fn:filter('ne','integration:some:excluded')",
                "_/_/things/twin/events?fn-filter=fn:filter(header:ditto-origin,'ne','chained-excluded-connection')" +
                        "|fn:filter(header:ditto-originator,'exists')",
                "_/_/things/live/messages?filter=gt(attributes/counter,42)" +
                        "&fn-filter=fn:filter(header:ditto-originator,'ne','integration:some:excluded')",
                "_/_/things/live/events?fn-filter=fn:filter(header:ditto-originator,'nope','integration:some:excluded')");

        // THEN the connection is created
        final String connectionId = parseIdFromResponse(connectionsClient()
                .postConnection(connection)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire());

        // AND the filters survive the round-trip
        try {
            connectionsClient().getConnection(connectionId)
                    .withDevopsAuth()
                    .expectingHttpStatus(HttpStatus.OK)
                    .expectingBody(satisfies(jsonString -> {
                        assertThat(String.valueOf(jsonString))
                                .contains("twin/events?fn-filter=fn:filter(header:ditto-originator,'ne'," +
                                        "'integration:some:excluded')");
                        assertThat(String.valueOf(jsonString))
                                .contains("twin/events?fn-filter=header:ditto-originator|fn:filter('ne'," +
                                        "'integration:some:excluded')");
                        assertThat(String.valueOf(jsonString))
                                .contains("fn:filter(header:ditto-origin,'ne','chained-excluded-connection')" +
                                        "|fn:filter(header:ditto-originator,'exists')");
                        assertThat(String.valueOf(jsonString))
                                .contains("gt(attributes/counter,42)&fn-filter=fn:filter(header:ditto-originator,'ne'");
                        assertThat(String.valueOf(jsonString))
                                .contains("fn:filter(header:ditto-originator,'nope'");
                    }))
                    .fire();
        } finally {
            // cleanup - also on assertion failure, so the connection never leaks
            connectionsClient().deleteConnection(connectionId)
                    .withDevopsAuth()
                    .expectingHttpStatus(HttpStatus.NO_CONTENT)
                    .fire();
        }
    }

    @Test
    public void modifyConnectionRevalidatesPipelineTargetTopicFilters() {
        // GIVEN the existing default connection (created in @Before, deleted in @After)
        final JsonObject existingConnection = JsonObject.of(connectionsClient()
                .getConnection(defaultConnectionId)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .fire()
                .getBody()
                .asString());

        // WHEN it is modified with a target topic 'fn-filter' referencing an unknown pipeline function
        // THEN the modification is rejected exactly like creation (modify runs the same ConnectionValidator)
        connectionsClient()
                .putConnection(defaultConnectionId.toString(), withTargetTopics(existingConnection,
                        "_/_/things/twin/events?fn-filter=fn:unknownfn('x')"))
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("connectivity:connection.configuration.invalid")
                .fire();

        // AND WHEN it is modified with a valid pipeline target topic filter
        // THEN the modification succeeds
        connectionsClient()
                .putConnection(defaultConnectionId.toString(), withTargetTopics(existingConnection,
                        "_/_/things/twin/events" +
                                "?fn-filter=fn:filter(header:ditto-originator,'ne','integration:some:excluded')"))
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    private void assertLogsNotEnabled() {
        final RetrieveConnectionLogsResponse logsResponse = retrieveLogs();

        assertThat(logsResponse.getEnabledSince()).isEmpty();
        assertThat(logsResponse.getEnabledUntil()).isEmpty();
        assertThat(logsResponse.getConnectionLogs()).isEmpty();
    }

    private void enableLogs() {
        connectionsClient().enableConnectionLogs(defaultConnectionId)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    private void assertLogsEnabled() {
        final RetrieveConnectionLogsResponse logsResponse = retrieveLogs();

        assertThat(logsResponse.getEnabledSince()).isNotEmpty();
        assertThat(logsResponse.getEnabledUntil()).isNotEmpty();
        assertThat(logsResponse.getConnectionLogs()).isEmpty();
    }

    private void assertLogsEnabledOnOpening() {
        final RetrieveConnectionLogsResponse logsResponse = retrieveLogs();

        assertThat(logsResponse.getEnabledSince()).isNotEmpty();
        assertThat(logsResponse.getEnabledUntil()).isNotEmpty();
        assertThat(logsResponse.getConnectionLogs()).isNotEmpty();
    }

    private void openConnection() {
        connectionsClient()
                .openConnection(defaultConnectionId.toString())
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    private void closeConnection() {
        connectionsClient()
                .closeConnection(defaultConnectionId.toString())
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    private void updateThing() {
        putAttribute(API_V_2, thingId, "foo", "\"bar\"")
                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.CREATED, HttpStatus.NO_CONTENT)
                .fire();
    }

    private Collection<LogEntry> assertLogEntries() {
        final RetrieveConnectionLogsResponse logsResponse = retrieveLogs();

        assertThat(logsResponse.getConnectionLogs()).isNotEmpty();
        return logsResponse.getConnectionLogs();
    }

    private void resetLogs() {
        connectionsClient().resetConnectionLogs(defaultConnectionId)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    private void assertLessLogEntries(final Collection<LogEntry> before) {
        final Collection<LogEntry> after = assertLogEntries();
        assertThat(after.size()).isLessThan(before.size());
    }

    private RetrieveConnectionLogsResponse retrieveLogs() {
        final Response response =
                connectionsClient().getConnectionLogs(defaultConnectionId)
                        .withDevopsAuth()
                        .expectingHttpStatus(HttpStatus.OK)
                        .fire();

        final JsonObject responseWithType = JsonObject.of(response.getBody().asString())
                .toBuilder()
                .set("type", RetrieveConnectionLogsResponse.TYPE)
                .set("status", 200)
                .build();
        return RetrieveConnectionLogsResponse.fromJson(responseWithType, DittoHeaders.empty());
    }

    private static JsonObject connectionWithTargetTopics(final String... topics) {
        return TestConstants.Connections.buildConnection().toBuilder()
                .set(Connection.JsonFields.TARGETS, JsonArray.of(JsonObject.newBuilder()
                        .set(Target.JsonFields.ADDRESS, "amqp/target1")
                        .set(Target.JsonFields.TOPICS, Arrays.stream(topics)
                                .map(JsonValue::of)
                                .collect(JsonCollectors.valuesToArray()))
                        .set(Target.JsonFields.AUTHORIZATION_CONTEXT,
                                JsonArray.of(JsonValue.of("integration:" +
                                        testingContext.getSolution().getUsername() + ":" +
                                        TestingContext.DEFAULT_SCOPE)))
                        .build()))
                .build();
    }

    private static JsonObject withTargetTopics(final JsonObject connection, final String... topics) {
        final JsonArray topicsArray = Arrays.stream(topics)
                .map(JsonValue::of)
                .collect(JsonCollectors.valuesToArray());
        final JsonArray targetsWithTopics = connection.getValue(Connection.JsonFields.TARGETS)
                .orElseThrow(() -> new AssertionError("connection has no targets: " + connection))
                .stream()
                .map(target -> (JsonValue) target.asObject().set(Target.JsonFields.TOPICS, topicsArray))
                .collect(JsonCollectors.valuesToArray());
        return connection.set(Connection.JsonFields.TARGETS, targetsWithTopics);
    }

}
