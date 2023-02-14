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
package org.eclipse.ditto.testing.system.connectivity;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.headers.translator.HeaderTranslator;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.json.Jsonifiable;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.events.Event;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.Credentials;
import org.eclipse.ditto.connectivity.model.SshTunnel;
import org.eclipse.ditto.connectivity.model.UserPasswordCredentials;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.messages.model.Message;
import org.eclipse.ditto.messages.model.MessageHeaders;
import org.eclipse.ditto.messages.model.MessagesModelFactory;
import org.eclipse.ditto.messages.model.signals.commands.SendFeatureMessage;
import org.eclipse.ditto.messages.model.signals.commands.SendFeatureMessageResponse;
import org.eclipse.ditto.messages.model.signals.commands.SendThingMessage;
import org.eclipse.ditto.messages.model.signals.commands.SendThingMessageResponse;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PolicyBuilder;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
import org.eclipse.ditto.protocol.adapter.ProtocolAdapter;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.Solution;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.client.ditto_protocol.HeaderBlocklistChecker;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PostMatcher;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.WithThingId;
import org.eclipse.ditto.things.model.signals.events.ThingEvent;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;

import io.restassured.response.Response;

/**
 * Abstract base class for Connectivity integration tests.
 *
 * @param <C> the consumer
 * @param <M> the consumer message
 */
public abstract class AbstractConnectivityITBase<C, M> extends IntegrationTest {

    // this protocol adapter filters nothing
    protected static final ProtocolAdapter PROTOCOL_ADAPTER = DittoProtocolAdapter.of(HeaderTranslator.empty());

    protected static final ConnectivityTestConfig CONFIG = ConnectivityTestConfig.getInstance();
    static final Map<String, Function<Connection, Connection>> MODS = new ConcurrentHashMap<>();

    protected static SshTunnel SSH_TUNNEL_CONFIG = getCommonSshTunnel(true);

    protected TestingContext testingContextWithRandomNs;
    protected String randomNamespace;
    protected String randomNamespace2;

    public static void addMod(final String id, final Function<Connection, Connection> mod) {
        if (MODS.containsKey(id)) {
            throw new IllegalArgumentException("Entry with id '" + id + "' already exists.");
        }
        MODS.put(id, mod);
    }

    static String connectionAuthIdentifier(final String username, final String suffix) {
        return String.format("%s:%s:%s", SubjectIssuer.INTEGRATION, username, suffix);
    }

    protected Subject connectionSubject(final String suffix) {
        return Subject.newInstance(SubjectIssuer.INTEGRATION,
                testingContextWithRandomNs.getSolution().getUsername() + ":" + suffix);
    }

    protected static AcknowledgementLabel connectionScopedAckLabel(final String connectionName,
            final String ackLabelSuffix, final ConnectivityFactory connectivityFactory) {
        final ConnectionId connectionId = connectivityFactory.getConnectionId(connectionName);
        return AcknowledgementLabel.of(MessageFormat.format("{0}:{1}", connectionId, ackLabelSuffix));
    }

    protected static ConnectionModelFactory connectionModelFactory;

    protected ConnectivityFactory cf;
    protected AbstractConnectivityITBase(final ConnectivityFactory cf) {
        this.cf = cf;
    }

    @BeforeClass
    public static void initStatic() {
        connectionModelFactory = new ConnectionModelFactory(AbstractConnectivityITBase::connectionAuthIdentifier);
    }

    public void setupConnectivity() throws Exception {
        randomNamespace = "org.eclipse.ditto.ns1." + randomString();
        randomNamespace2 = "org.eclipse.ditto.ns2." + randomString();
        final Solution solution = serviceEnv.createSolution(randomNamespace);
        testingContextWithRandomNs = TestingContext.withGeneratedMockClient(solution, TEST_CONFIG);
        cf = cf.withSolutionSupplier(() -> testingContextWithRandomNs.getSolution())
                .withAuthClient(testingContextWithRandomNs.getOAuthClient());
    }

    protected static Response deleteConnection(final ConnectionId connectionId) {
        return connectionsClient().deleteConnection(connectionId)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    protected static List<ConnectionId> getAllConnectionIds(final String connectionNamePrefixToMatch) {
        final Response response = connectionsClient().getConnectionIds()
                .withDevopsAuth()
                .fire();

        return JsonFactory.newArray(response.body().asString()).stream()
                .filter(JsonValue::isObject)
                .map(JsonValue::asObject)
                .filter(conObj -> conObj.getValue(Connection.JsonFields.ID)
                        .filter(id -> id.startsWith(connectionNamePrefixToMatch))
                        .isPresent()
                )
                .map(idAndName -> ConnectionId.of(idAndName.getValueOrThrow(Connection.JsonFields.ID)))
                .collect(Collectors.toList());
    }

    protected static List<Connection> getAllConnections(final String connectionNamePrefixToMatch) {
        final Response response = connectionsClient().getConnections()
                .withDevopsAuth()
                .fire();

        return JsonFactory.newArray(response.body().asString()).stream()
                .filter(JsonValue::isObject)
                .map(JsonValue::asObject)
                .filter(connectionJson -> connectionJson.getValue(Connection.JsonFields.NAME)
                        .filter(name -> name.startsWith(connectionNamePrefixToMatch))
                        .isPresent()
                )
                .map(ConnectivityModelFactory::connectionFromJson)
                .collect(Collectors.toList());
    }

    protected static Connection getConnectionExistingByName(final String connectionName) {
        final List<Connection> allConnections = getAllConnections(connectionName);

        return allConnections.stream()
                .filter(conn -> connectionName.equals(conn.getName().orElse(null)))
                .findAny()
                .orElseThrow(() -> {
                    LOGGER.error("Connection with name <" + connectionName + "> does not " +
                            "exist, existing were connections:\n{}", allConnections);
                    return new IllegalStateException("Connection with name <" + connectionName + "> does not " +
                            "exist");
                });
    }

    protected static GetMatcher getConnectionMatcher(final CharSequence connectionId) {
        return connectionsClient().getConnection(connectionId)
                .withDevopsAuth();
    }

    protected static void testConnectionWithErrorExpected(final JsonObject connectionJson,
            final String errorString) {

        testConnection(connectionJson)
                .expectingBody(Matchers.containsString(errorString))
                .fire();
    }

    private static PostMatcher testConnection(final JsonObject connection) {
        final JsonObject connectionJsonStrWithoutId = removeIdFromJson(connection);

        return connectionsClient().testConnection(connectionJsonStrWithoutId)
                .withDevopsAuth();
    }

    /**
     * Setting a connectionID is not allowed via post, remove it with this method.
     *
     * @param connection the connection (in JSON format)
     * @return the connection string without connectionId
     */
    private static JsonObject removeIdFromJson(final JsonObject connection) {
        return connection.remove(Connection.JsonFields.ID.getPointer());
    }

    protected static String createNewCorrelationId() {
        return UUID.randomUUID().toString();
    }

    static DittoHeaders createDittoHeaders(final String correlationId) {
        return DittoHeaders.newBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .correlationId(correlationId)
                .build();
    }

    static void waitShort() {
        waitMillis(100);
    }

    @SuppressWarnings("squid:S2925") // suppress Thread.sleep-warning
    static void waitMillis(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static String messagesResourceForToFeatureV2(final ThingId thingId, final String featureId,
            final String messageTopic) {
        return TestConstants.Messages.MESSAGES_API_2_BASE_URL + TestConstants.Things.THINGS_PATH + "/" + thingId +
                TestConstants.Things.FEATURES_PATH + "/" + featureId + TestConstants.Messages.INBOX_PATH
                + TestConstants.Messages.MESSAGES_PATH + "/" + messageTopic;
    }

    protected abstract AbstractConnectivityWorker<C, M> getConnectivityWorker();

    protected C initResponseConsumer(final String connectionName, final String correlationId) {
        return getConnectivityWorker().initResponseConsumer(connectionName, correlationId);
    }

    protected C initTargetsConsumer(final String connectionName) {
        return getConnectivityWorker().initTargetsConsumer(connectionName);
    }

    protected M consumeResponse(final String correlationId, final C consumer) {
        return getConnectivityWorker().consumeResponse(correlationId, consumer);
    }

    CompletableFuture<M> consumeResponseInFuture(final String correlationId, final C consumer) {
        return getConnectivityWorker().consumeResponseInFuture(correlationId, consumer);
    }

    protected C initTargetsConsumer(final String connectionName, final String targetAddress) {
        return getConnectivityWorker().initTargetsConsumer(connectionName, targetAddress);
    }

    protected M consumeFromTarget(final String connectionName, final C targetsConsumer) {
        return getConnectivityWorker().consumeFromTarget(connectionName, targetsConsumer);
    }

    CompletableFuture<M> consumeFromTargetInFuture(final String connectionName, final C targetsConsumer) {
        return getConnectivityWorker().consumeFromTargetInFuture(connectionName, targetsConsumer);
    }

    String getCorrelationId(final M message) {
        return getConnectivityWorker().getCorrelationId(message);
    }

    protected void sendSignal(final String connectionName, final Signal<?> signal) {
        final String correlationId = signal.getDittoHeaders().getCorrelationId().get();
        final Adaptable adaptable = PROTOCOL_ADAPTER.toAdaptable(signal);
        final String stringMessage = ProtocolFactory.wrapAsJsonifiableAdaptable(adaptable).toJsonString();
        final Map<String, String> headersMap = adaptable.getDittoHeaders().asCaseSensitiveMap();

        sendAsJsonString(connectionName, correlationId, stringMessage, headersMap);
    }

    protected void sendAsJsonString(final String connectionName, final String correlationId, final String stringMessage,
            final Map<String, String> headersMap) {

        getConnectivityWorker().sendAsJsonString(connectionName, correlationId, stringMessage, headersMap);
    }

    void sendMessageAsJsonString(final String connectionName,
            final String stringMessage,
            final MessageHeaders messageHeaders) {

        final String correlationId = messageHeaders.getCorrelationId().orElseThrow();
        if (protocolSupportsHeaders()) {
            getConnectivityWorker().sendAsJsonString(connectionName, correlationId, stringMessage, messageHeaders);
        } else {
            // protocol does not support headers; construct signals according to message headers
            // and hope the connections decode them as Ditto protocol messages.
            final Signal<?> signal = constructMessageCommandOrResponse(stringMessage, messageHeaders);
            sendSignal(connectionName, signal);
        }
    }

    void sendMessageAsBytePayload(final String connectionName, final byte[] byteMessage,
            final MessageHeaders messageHeaders) {
        final String correlationId = messageHeaders.getCorrelationId().orElseThrow();
        if (protocolSupportsHeaders()) {
            getConnectivityWorker().sendAsBytePayload(connectionName, correlationId, byteMessage, messageHeaders);
        } else {
            // protocol does not support headers; construct signals according to message headers
            // and hope the connections decode them as Ditto protocol messages.
            final Signal<?> signal = constructMessageCommandOrResponse(byteMessage, messageHeaders);
            sendSignal(connectionName, signal);
        }
    }

    protected JsonifiableAdaptable jsonifiableAdaptableFrom(final M message) {
        return getConnectivityWorker().jsonifiableAdaptableFrom(message);
    }

    String textFrom(final M message) {
        return getConnectivityWorker().textFrom(message);
    }

    byte[] bytesFrom(final M message) {
        return getConnectivityWorker().bytesFrom(message);
    }

    Map<String, String> checkHeaders(final M message) {
        return getConnectivityWorker()
                .checkHeaders(message, HeaderBlocklistChecker::assertHeadersDoNotIncludeBlocklisted);
    }

    /**
     * Whether the connectivity protocol supports headers. If it does not, headers from javascript mapper are lost.
     *
     * @return whether headers are supported.
     */
    protected boolean protocolSupportsHeaders() {
        return true;
    }

    protected <T> T expectMsgClass(final Class<T> clazz, final String connectionName, final C messageConsumer) {
        final M message = consumeFromTarget(connectionName, messageConsumer);
        final Signal<?> signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(message));
        assertThat(signal).isInstanceOf(clazz);
        return clazz.cast(signal);
    }

    /**
     * Expects, that either no message, or a message of type {@code clazz} is received.
     */
    @Nullable
    protected <T> T expectMsgClassOrNothing(final Class<T> clazz,
            final String connectionName,
            final C messageConsumer) {

        final M message = consumeFromTarget(connectionName, messageConsumer);
        if (null == message) {
            return clazz.cast(null);
        }
        final Signal<?> signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(message));
        assertThat(signal).isInstanceOf(clazz);
        return clazz.cast(signal);
    }

    protected <T extends WithThingId> void consumeAndAssert(final String connectionName, final ThingId thingId,
            final String correlationId,
            final C messageConsumer,
            final Class<T> expectedMessageClass,
            final Consumer<T> additionalAssertionsConsumer) {

        LOGGER.info("Waiting for thingSignal for Thing <{}> of class <{}> ...", thingId, expectedMessageClass);

        M message;
        do {
            // skip over messages with unexpected correlation ID, e. g., ThingCreated events
            message = consumeFromTarget(connectionName, messageConsumer);
            assertThat(message)
                    .withFailMessage("Did not receive any message for correlation ID " + correlationId)
                    .isNotNull();
            LOGGER.info("Got message with correlation ID <{}>; expecting message with correlation ID <{}>.",
                    getCorrelationId(message), correlationId);
        } while (!correlationId.equals(getCorrelationId(message)));

        checkHeaders(message);

        final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(message);
        final Adaptable messageAdaptable1 = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
        final Jsonifiable<JsonObject> thingSignal = PROTOCOL_ADAPTER.fromAdaptable(messageAdaptable1);

        assertThat(thingSignal).isInstanceOf(WithThingId.class);
        assertThat(thingSignal).isInstanceOf(expectedMessageClass);
        assertThat((CharSequence) ((WithThingId) thingSignal).getEntityId()).isEqualTo(thingId);
        additionalAssertionsConsumer.accept(expectedMessageClass.cast(thingSignal));
    }

    CompletableFuture<Void> consumeAndAssertEventsInFuture(final String connectionName, final C eventConsumer,
            final List<Consumer<Jsonifiable<JsonObject>>> expectedMessageAsserts,
            final String... eventDescriptions) {
        return CompletableFuture.runAsync(
                () -> consumeAndAssertEvents(connectionName, eventConsumer, expectedMessageAsserts, eventDescriptions));
    }

    void consumeAndAssertEvents(final String connectionName, final C eventConsumer,
            final List<Consumer<Jsonifiable<JsonObject>>> expectedMessageAsserts,
            final String... eventDescriptions) {

        for (int i = 0; i < expectedMessageAsserts.size(); ++i) {
            final Consumer<Jsonifiable<JsonObject>> expectedMessageAssert = expectedMessageAsserts.get(i);
            final M eventMessage = consumeFromTarget(connectionName, eventConsumer);
            // will break the loop in timeout case
            assertThat(eventMessage)
                    .withFailMessage(
                            "Did not receive " + (i < eventDescriptions.length ? eventDescriptions[i] : "an Event"))
                    .isNotNull();

            final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(eventMessage);
            final Adaptable eventAdaptable1 = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
            final Jsonifiable<JsonObject> event = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable1);
            LOGGER.info("Received event, running its assertions: " + event);
            expectedMessageAssert.accept(event);
        }
    }

    @SuppressWarnings("unchecked")
    <T extends ThingEvent> T thingEventForJson(final Jsonifiable<JsonObject> thingEvent,
            final Class<T> eventClass, @Nullable final String correlationId, final ThingId thingId) {
        assertThat(thingEvent).isInstanceOf(eventClass);
        assertThat(thingEvent).isInstanceOf(Event.class);

        final T castedEvent = (T) thingEvent;

        if (correlationId != null) {
            assertThat(castedEvent.getDittoHeaders().getCorrelationId()).contains(correlationId);
        }
        assertThat((CharSequence) castedEvent.getEntityId()).isEqualTo(thingId);
        return castedEvent;
    }

    protected PolicyId putPolicyForThing(final ThingId thingId, final String... authorizedConnectionNames) {
        final PolicyId policyId = PolicyId.of(thingId);
        final PolicyBuilder.LabelScoped policyBuilder = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("default")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject());

        for (final String name : authorizedConnectionNames) {
            policyBuilder.setSubject(connectionSubject(name));
        }

        policyBuilder.setGrantedPermissions("thing", "/", "READ", "WRITE")
                .setGrantedPermissions("policy", "/", "READ", "WRITE")
                .setGrantedPermissions("message", "/", "READ", "WRITE");
        putPolicy(policyId, policyBuilder.build())
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        return policyId;
    }

    protected static RuntimeException mapException(final Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        } else {
            return new IllegalStateException(e);
        }
    }

    private static void cleanupSingleConnectionById(final ConnectionId connectionId) {
        LOGGER.info("Removing connection with ID <{}>", connectionId);
        final Response response = deleteConnection(connectionId);
        LOGGER.info("cleanupSingleConnection for connection with ID <{}>. Response: {}",
                connectionId, response.getStatusLine());
    }

    protected static void cleanupConnections(final String username) {
        LOGGER.info("Removing all dynamic connections for solution username <{}>", username);
        final List<ConnectionId> connectionIds = getAllConnectionIds(username);
        LOGGER.info("Will delete connectionIds with IDs: <{}>", connectionIds);

        connectionIds.forEach(AbstractConnectivityITBase::cleanupSingleConnectionById);
    }

    private static Signal<?> constructMessageCommandOrResponse(final String payload, final MessageHeaders headers) {
        final Message<String> message = MessagesModelFactory
                .<String>newMessageBuilder(headers)
                .payload(payload)
                .build();
        return constructMessageCommandOrResponse(message, headers);
    }

    private static Signal<?> constructMessageCommandOrResponse(final byte[] payload, final MessageHeaders headers) {

        final Message<byte[]> message = MessagesModelFactory
                .<byte[]>newMessageBuilder(headers)
                .rawPayload(ByteBuffer.wrap(payload))
                .build();
        return constructMessageCommandOrResponse(message, headers);
    }

    private static Signal<?> constructMessageCommandOrResponse(final Message<?> message, final MessageHeaders headers) {
        final boolean isCommand = headers.getHttpStatus().isEmpty();
        final boolean isThingMessage = headers.getFeatureId().isEmpty();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(headers.getCorrelationId().orElseThrow())
                .contentType(headers.getContentType().orElseThrow())
                .build();
        final Signal<?> signal;
        if (isCommand && isThingMessage) {
            signal = SendThingMessage.of(headers.getEntityId(), message, dittoHeaders);
        } else if (isCommand) {
            signal = SendFeatureMessage.of(headers.getEntityId(), headers.getFeatureId().orElseThrow(),
                    message, dittoHeaders);
        } else if (isThingMessage) {
            signal = SendThingMessageResponse.of(headers.getEntityId(), message,
                    headers.getHttpStatus().orElseThrow(), dittoHeaders);
        } else {
            signal = SendFeatureMessageResponse.of(headers.getEntityId(), headers.getFeatureId().orElseThrow(),
                    message, headers.getHttpStatus().orElseThrow(), dittoHeaders);
        }
        return signal;
    }

    protected final class DefaultTestWatcher extends TestWatcher {

        private final Logger logger;

        public DefaultTestWatcher(final Logger logger) {
            this.logger = logger;
        }

        @Override
        protected void starting(final org.junit.runner.Description description) {
            logger.info("Testing: {}()", description.getMethodName());
        }

        @Override
        protected void finished(final Description description) {
            logger.info("Finished: {}()", description.getMethodName());
        }

        @Override
        protected void failed(final Throwable e, final org.junit.runner.Description description) {
        }

    }

    private static String randomString() {
        return RandomStringUtils.randomAlphabetic(1) + RandomStringUtils.randomAlphanumeric(10);
    }

    private static SshTunnel getCommonSshTunnel(final boolean enabled) {
        final String uri = String.format("ssh://%s:%d", CONFIG.getSshHostname(), CONFIG.getSshPort());
        final String username = CONFIG.getSshUsername();
        final String password = CONFIG.getSshPassword();
        final String fingerprint = CONFIG.getSshFingerprint();
        final Credentials credentials = UserPasswordCredentials.newInstance(username, password);
        return ConnectivityModelFactory.newSshTunnelBuilder(enabled, credentials, uri)
                .validateHost(true)
                .knownHosts(List.of(fingerprint))
                .build();
    }

}
