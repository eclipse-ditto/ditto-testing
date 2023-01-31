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
package org.eclipse.ditto.testing.system.connectivity.mqtt3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;

import java.net.InetSocketAddress;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.json.Jsonifiable;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.commands.CommandResponse;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.Enforcement;
import org.eclipse.ditto.connectivity.model.HeaderMapping;
import org.eclipse.ditto.connectivity.model.SourceBuilder;
import org.eclipse.ditto.connectivity.model.TargetBuilder;
import org.eclipse.ditto.connectivity.model.signals.commands.exceptions.ConnectionFailedException;
import org.eclipse.ditto.connectivity.model.signals.commands.exceptions.ConnectionUnavailableException;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonObjectBuilder;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.client.ConnectionsClient;
import org.eclipse.ditto.testing.common.matcher.StatusCodeSuccessfulMatcher;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityITestCases;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityWorker;
import org.eclipse.ditto.testing.system.connectivity.ConnectionCategory;
import org.eclipse.ditto.testing.system.connectivity.Connections;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityConstants;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityFactory;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityTestConfig;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityTestWebsocketClient;
import org.eclipse.ditto.testing.system.connectivity.UseConnection;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.ThingErrorResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThingResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.DeleteThing;
import org.eclipse.ditto.things.model.signals.events.ThingCreated;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;

import io.restassured.response.Response;

/**
 * Integration tests for Connectivity of MQTT 3 connections.
 * <p>
 * Certificates used by this test expires on 01 January 2100. Please regenerate certificates according to {@code
 * /integration/connectivity/service/src/test/resources/mqtt/README.md}.
 * </p>
 */
public final class Mqtt3ConnectivitySuite
        extends AbstractConnectivityITestCases<BlockingQueue<Mqtt3Publish>, Mqtt3Publish> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Mqtt3ConnectivitySuite.class);

    private static final ConnectionType MQTT_TYPE = ConnectionType.MQTT;
    private static final String MQTT_EXTERNAL_URI = getMqttUri(CONFIG);
    private static final String MQTT_INTERNAL_URI = getMqttInternalTcpUri(CONFIG);
    private static final String MQTT_TUNNEL_URI = getMqttTunnelUri(CONFIG);

    private static final long WAIT_TIMEOUT_MS = 15_000L;
            // if after 10secs waiting time no message arrived, handle as "no message consumed"

    private static final ConnectivityFactory.GetEnforcement MQTT_SOURCE_ENFORCEMENT =
            connectionName -> ConnectivityModelFactory.newSourceAddressEnforcement(
                    connectionName + "/source/"
                            +
                            "{{entity:id | fn:substring-before(':') | fn:substring-before('not-matching') | fn:default(entity:namespace)}}/"
                            + "{{ entity:id | fn:substring-after(\":\") | fn:default('should-not-happen') }}");

    private static final String ADD_MQTT_TARGET_HEADER_MAPPING = "ADD_MQTT_TARGET_HEADER_MAPPING";
    private static final String ADD_MQTT_SOURCE_HEADER_MAPPING = "ADD_MQTT_SOURCE_HEADER_MAPPING";
    private static final String REMOVE_MQTT_HEADER_MAPPINGS = "REMOVE_MQTT_MAPPINGS";

    static {
        // adds mqtt source header mapping
        addMod(ADD_MQTT_SOURCE_HEADER_MAPPING, connection -> {
                    final HeaderMapping headerMapping = ConnectivityModelFactory.newHeaderMapping(Map.of(
                            "custom.topic", "{{ header:mqtt.topic }}",
                            "custom.qos", "{{ header:mqtt.qos }}",
                            "custom.retain", "{{ header:mqtt.retain }}"
                    ));
                    return connection.toBuilder()
                            .setSources(connection.getSources().stream()
                                    .map(ConnectivityModelFactory::newSourceBuilder)
                                    .map(sb -> sb.headerMapping(headerMapping))
                                    .map(SourceBuilder::build)
                                    .collect(Collectors.toList())).build();
                }
        );
        // adds mqtt target header mapping
        addMod(ADD_MQTT_TARGET_HEADER_MAPPING, connection -> {
                    final HeaderMapping headerMapping = ConnectivityModelFactory.newHeaderMapping(Map.of(
                            "mqtt.topic", "{{ header:custom.topic }}",
                            "mqtt.qos", "{{ header:custom.qos }}",
                            "mqtt.retain", "{{ header:custom.retain }}"
                    ));
                    return connection.toBuilder()
                            .setTargets(connection.getTargets().stream()
                                    .map(ConnectivityModelFactory::newTargetBuilder)
                                    .map(sb -> sb.headerMapping(headerMapping))
                                    .map(TargetBuilder::build)
                                    .collect(Collectors.toList())).build();
                }
        );
        // removes mqtt header mappings
        addMod(REMOVE_MQTT_HEADER_MAPPINGS, connection -> connection.toBuilder()
                .setSources(connection.getSources().stream()
                        .map(ConnectivityModelFactory::newSourceBuilder)
                        .map(sb -> sb.headerMapping(null))
                        .map(SourceBuilder::build)
                        .collect(Collectors.toList()))
                .setTargets(connection.getTargets().stream()
                        .map(ConnectivityModelFactory::newTargetBuilder)
                        .map(sb -> sb.headerMapping(null))
                        .map(TargetBuilder::build)
                        .collect(Collectors.toList())).build()
        );
    }

    private final String mqttCustomConnectionName;

    private final Mqtt3ConnectivityWorker connectivityWorker;
    private Mqtt3AsyncClient mqttClient;

    public Mqtt3ConnectivitySuite() {
        super(ConnectivityFactory.of(
                        "Mqtt3",
                        connectionModelFactory,
                        () -> SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution(),
                        MQTT_TYPE,
                        (tunnel, basicAuth) -> tunnel ? MQTT_TUNNEL_URI : MQTT_INTERNAL_URI,
                        Collections::emptyMap,
                        Mqtt3ConnectivitySuite::getTargetTopic,
                        Mqtt3ConnectivitySuite::getSourceTopic,
                        MQTT_SOURCE_ENFORCEMENT,
                        () -> SSH_TUNNEL_CONFIG,
                        SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient())
                .withMaxClientCount(1));

        mqttCustomConnectionName = cf.disambiguate("Mqtt3Custom");
        connectivityWorker = new Mqtt3ConnectivityWorker(LOGGER, Mqtt3ConnectivitySuite::getTargetTopic,
                () -> mqttClient, cf.connectionNameWithEnforcementEnabled, Duration.ofMillis(WAIT_TIMEOUT_MS));
    }

    private static ConnectivityTestWebsocketClient websocketClient;

    @BeforeClass
    public static void setUpWebsocketClient() {
        websocketClient = ConnectivityTestWebsocketClient.newInstance(thingsWsUrl(TestConstants.API_V_2),
                SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getAccessToken());
        websocketClient.connect("mqtt-websocket-" + UUID.randomUUID());
    }

    @AfterClass
    public static void closeWebsocketClient() {
        if (websocketClient != null) {
            websocketClient.disconnect();
            websocketClient = null;
        }
    }

    @Override
    protected AbstractConnectivityWorker<BlockingQueue<Mqtt3Publish>, Mqtt3Publish> getConnectivityWorker() {
        return connectivityWorker;
    }

    @Before
    public void createConnections() throws Exception {
        LOGGER.info("Creating connections..");
        cf.setUpConnections(connectionsWatcher.getConnections(),
                Collections.singleton(setupSingleConnectionWithTopicFilter(mqttCustomConnectionName)));
    }

    @Before
    public void setup() {

        disconnectClient();
        final String clientId = getClass().getSimpleName() + UUID.randomUUID();
        final String uri = MQTT_EXTERNAL_URI;
        LOGGER.info("Connecting MQTT client <{}> to <{}>", clientId, uri);
        mqttClient = Mqtt3Client.builder()
                .serverAddress(new InetSocketAddress(CONFIG.getMqttHostName(), CONFIG.getMqttPortTcp()))
                .identifier(clientId)
                .buildAsync();
        mqttClient.connect(); // session is clean by default
    }

    @After
    public void cleanup() {
        disconnectClient();
        cleanupConnections(SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution());
    }

    @Override
    protected boolean protocolSupportsHeaders() {
        return false;
    }

    @Test
    @Connections(ConnectionCategory.CONNECTION1)
    public void filterIncomingMqttMessagesByTopicFilters() {
        final ThingId thingId1 = generateThingId();
        final ThingId thingId2 = generateThingId();
        final String correlationId = createNewCorrelationId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId1)
                .build();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(connectionSubject(cf.connectionName1))
                .setSubject(connectionSubject(mqttCustomConnectionName))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .correlationId(correlationId)
                .build();
        final DittoHeaders dittoHeadersForCleanup = dittoHeaders.toBuilder()
                .correlationId(UUID.randomUUID().toString())
                .build();
        final CreateThing createThing1 = CreateThing.of(thing, policy.toJson(), dittoHeaders);
        final CreateThing createThing2 = CreateThing.of(thing.toBuilder().setId(thingId2).build(), null, dittoHeaders);

        final BlockingQueue<Mqtt3Publish> responseConsumer = initResponseConsumer(cf.connectionName1, correlationId);

        // send createThing2 to topic of thingId1 and expect it rejected by filter "<CONNECTION_NAME_WITH_NAMESPACE_AND_RQL_FILTER>/{{thing:id}}/src"
        final String thingId1TopicPrefix = mqttCustomConnectionName + "/" + thingId1;
        sendSignal(thingId1TopicPrefix, createThing2);
        final JsonifiableAdaptable jsonifiableAdaptable2 =
                jsonifiableAdaptableFrom(consumeResponse(correlationId, responseConsumer));
        final Jsonifiable<JsonObject> response2 = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptable2);
        if (!response2.toJson().getValue("type").equals(Optional.of(JsonValue.of(ThingErrorResponse.TYPE)))) {
            sendSignal(thingId1TopicPrefix, DeleteThing.of(thingId2, dittoHeadersForCleanup));
            throw new AssertionError(MessageFormat.format(
                    "Topic filter <{0}> did not prevent command at topic <{1}>: <{2}>. Response: <{3}>",
                    getTopicFilter(mqttCustomConnectionName), getSourceTopic(thingId1TopicPrefix), createThing2,
                    response2));
        }

        // send createThing1 to topic of thingId1 and expect it to pass through "<CONNECTION_NAME_WITH_NAMESPACE_AND_RQL_FILTER>/{{thing:id}}/src"
        sendSignal(thingId1TopicPrefix, createThing1);
        final JsonifiableAdaptable jsonifiableAdaptable1 =
                jsonifiableAdaptableFrom(consumeResponse(correlationId, responseConsumer));
        final Jsonifiable<JsonObject> response1 = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptable1);
        assertThat(response1.toJson().getValue("type")).contains(JsonValue.of(CreateThingResponse.TYPE));
        sendSignal(mqttCustomConnectionName, DeleteThing.of(thingId1, dittoHeadersForCleanup));
    }

    @Test
    @Connections(ConnectionCategory.NONE)
    public void testClientCertificateAuthentication() throws Exception {
        final String connectionName = createMqttOverSslConnection(CONFIG.getMqttCACrt(), CONFIG.getMqttClientCrt(),
                CONFIG.getMqttClientKey());

        sendCreateThingAndEnsureResponseIsSentBack(connectionName);
    }

    @Test
    @Connections(ConnectionCategory.NONE)
    public void testServerCertificateVerificationFailure() {
        testSslFailure(null, CONFIG.getMqttClientCrt(), CONFIG.getMqttClientKey());
    }

    @Test
    @Connections(ConnectionCategory.NONE)
    public void testClientCertificateVerificationFailure() {
        testSslFailure(CONFIG.getMqttCACrt(), CONFIG.getMqttUnsignedCrt(), CONFIG.getMqttUnsignedKey());
    }

    @Test
    @Connections(ConnectionCategory.NONE)
    public void testSslFailureDueToUnidentifiedClient() {
        testConnectionWithErrorExpected(SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution(),
                getMqttOverSslConnection(String.valueOf(UUID.randomUUID()), CONFIG.getMqttCACrt(), null, null),
                ConnectionUnavailableException.ERROR_CODE);
    }

    @Test
    @Connections(ConnectionCategory.NONE)
    public void creatingMqttConnectionWithHeaderMappingShouldFail() {
        final String username = SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution().getUsername();
        final String connectionName = UUID.randomUUID().toString();
        final Connection connection = connectionModelFactory.buildConnectionModelWithHeaderMapping(
                username,
                connectionName,
                MQTT_TYPE,
                MQTT_INTERNAL_URI,
                Collections.emptyMap(),
                getSourceTopic(connectionName),
                getTargetTopic(connectionName));

        final JsonObject connectionWithoutId = ConnectivityFactory.removeIdFromJson(connection.toJson());

        ConnectionsClient.getInstance()
                .postConnection(connectionWithoutId)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("connectivity:connection.configuration.invalid")
                // only the headers mqtt.topic, mqtt.qos and mqtt.retain are allowed in mqtt header mapping
                .expectingBody(Matchers.containsString("Some placeholders could not be resolved."))
                .fire();
    }

    @Test
    @UseConnection(category = ConnectionCategory.CONNECTION1, mod = ADD_MQTT_SOURCE_HEADER_MAPPING)
    public void allowedHeadersAreMappedFromInboundMqttMessage() {
        final AtomicBoolean waitForEvent = new AtomicBoolean(false);
        final CompletableFuture<Void> waitForAck = websocketClient.startConsumingEvents(event -> {
            // custom.* headers are mapped by the configured header mapping
            final String sourceTopic = cf.connectionName1 + "/source";
            assertThat(event.getDittoHeaders()).containsEntry("custom.topic", sourceTopic);
            assertThat(event.getDittoHeaders()).containsEntry("custom.qos", "1");
            assertThat(event.getDittoHeaders()).containsEntry("custom.retain", "false");

            // mqtt.* headers are not mapped by default
            assertThat(event.getDittoHeaders()).doesNotContainKeys("mqtt.topic", "mqtt.qos", "mqtt.retain");

            waitForEvent.set(true);
        });
        Awaitility.await("waiting for subscription ack on websocket")
                .atMost(Duration.ofSeconds(30))
                .until(waitForAck::isDone);

        sendCreateThing(cf.connectionName1, ThingBuilder.FromScratch::build);
        Awaitility.await("waiting for event sent via mqtt").untilTrue(waitForEvent);
    }

    @Test
    @UseConnection(category = ConnectionCategory.CONNECTION1, mod = ADD_MQTT_TARGET_HEADER_MAPPING)
    public void allowedHeadersAreMappedToOutboundMqttMessage() throws InterruptedException {

        final CreateThing createThing = buildCreateThing(cf.connectionName1, ThingBuilder.FromScratch::build);
        final String customTopic = "some/custom/topic";

        final CreateThing createThingWithAdditionalHeaders = createThing.setDittoHeaders(createThing
                .getDittoHeaders()
                .toBuilder()
                .putHeader("custom.topic", customTopic) // is mapped to mqtt.topic which is applied to the mqtt message
                .putHeader("custom.qos", "1") // is mapped to mqtt.qos which is applied to the mqtt message
                .putHeader("custom.retain", "true") // is mapped to mqtt.retain which is applied to the mqtt message
                .build());

        final CompletableFuture<CommandResponse<?>> response = websocketClient.send(createThingWithAdditionalHeaders);
        Awaitility.await("waiting for websocket response").until(response::isDone);

        // subscribe to custom topic *after* sending the message because the message is sent as retained i.e. it is
        // stored in the broker and published to every new subscriber on the custom topic
        // must subscribe with qos=1 to check if custom.qos was applied
        final BlockingQueue<Mqtt3Publish> messages = connectivityWorker.subscribe(customTopic, 1);
        final Mqtt3Publish message = messages.poll(5, TimeUnit.SECONDS);

        assertThat(message).isNotNull();
        assertThat(message.isRetain()).isTrue();
        assertThat(message.getQos()).isEqualTo(MqttQos.AT_LEAST_ONCE);

        final Signal<?> signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(message));
        assertThat(signal).isInstanceOf(ThingCreated.class);
        final ThingCreated thingCreated = (ThingCreated) signal;
        assertThat((Object) thingCreated.getEntityId()).isEqualTo(createThing.getEntityId());
    }

    private void testSslFailure(@Nullable final String caCrt,
            @Nullable final String clientCrt,
            @Nullable final String clientKey) {

        final String connectionName = UUID.randomUUID().toString();
        final JsonObject connectionStr = getMqttOverSslConnection(connectionName, caCrt, clientCrt, clientKey);

        testConnectionWithErrorExpected(SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution(), connectionStr,
                ConnectionFailedException.ERROR_CODE);
    }


    @Override
    protected String targetAddressForTargetPlaceHolderSubstitution() {
        return String.format("%s%s", getTargetTopic(cf.connectionNameWithAuthPlaceholderOnHEADER_ID), ConnectivityConstants.TARGET_SUFFIX);
    }

    @Override
    @SuppressWarnings("squid:S2699")
    public void sendSingleCommandAndEnsurePlaceholderSubstitution() {
        LOGGER.info("Test succeeds trivially because MQTT connections do not support placeholder substitution " +
                "in source authorization subjects.");
    }

    @Override
    @SuppressWarnings("squid:S2699")
    public void ensureSourceAuthenticationPlaceholderSubstitution() {
        LOGGER.info("Test succeeds trivially because MQTT connections do not support placeholder substitution " +
                "in source authorization subjects.");
    }

    @Override
    @Ignore("Not possible to test because we need to create JSON from the message we're sending and this will fail " +
            "before even sending it.")
    public void createThingWithMultipleSlashesInFeatureProperty() {
    }

    @Override
    @Test
    @UseConnection(category = ConnectionCategory.CONNECTION_WITH_CONNECTION_ANNOUNCEMENTS, mod = REMOVE_MQTT_HEADER_MAPPINGS)
    public void sendsConnectionAnnouncements() {
        LOGGER.info("Running sendsConnectionAnnouncements without header mappings for MQTT");
        super.sendsConnectionAnnouncements();
    }

    private void disconnectClient() {
        if (mqttClient != null) {
            if (mqttClient.getState().equals(MqttClientState.CONNECTED)) {
                mqttClient.disconnect();
            }
            mqttClient = null;
        }
    }

    private Connection setupSingleConnectionWithTopicFilter(final String connectionName) {
        LOGGER.info("Creating an MQTT connection with topic filter and name <{}> to <{}>", connectionName,
                MQTT_INTERNAL_URI);
        final String sourceAddress = connectionName + "/#";
        final String targetAddress = getTargetTopic(connectionName);

        return connectionModelFactory.buildConnectionModelWithEnforcement(
                SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution().getUsername(),
                connectionName, MQTT_TYPE,
                MQTT_INTERNAL_URI,
                Collections.emptyMap(),
                sourceAddress,
                targetAddress,
                getTopicEnforcement(getTopicFilter(connectionName)));
    }

    private String createMqttOverSslConnection(@Nullable final String caCrt,
            @Nullable final String clientCrt,
            @Nullable final String clientKey) throws InterruptedException, ExecutionException, TimeoutException {

        final String connectionName = UUID.randomUUID().toString();
        final JsonObject connection = getMqttOverSslConnection(connectionName, caCrt, clientCrt, clientKey);
        final Response response = cf.asyncCreateConnection(SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution(), connection)
                .get(30, TimeUnit.SECONDS);
        LOGGER.info("Created MQTT connection over SSL <{}> Response: {}", connectionName, response.statusLine());
        assertThat(response.getStatusCode()).satisfies(StatusCodeSuccessfulMatcher.getConsumer());
        return connectionName;
    }

    private static JsonObject getMqttOverSslConnection(final String connectionName,
            @Nullable final String caCrt,
            @Nullable final String clientCrt,
            @Nullable final String clientKey) {

        final JsonObjectBuilder credentialsJsonBuilder = JsonFactory.newObjectBuilder();
        credentialsJsonBuilder.set("type", "client-cert");
        if (clientCrt != null) {
            credentialsJsonBuilder.set("cert", clientCrt);
        }
        if (clientKey != null) {
            credentialsJsonBuilder.set("key", clientKey);
        }
        final JsonObject credentialsJson = credentialsJsonBuilder.build();


        final Connection connectionTemplate = connectionModelFactory.buildConnectionModel(
                SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution().getUsername(),
                connectionName,
                MQTT_TYPE,
                getMqttInternalSslUri(CONFIG),
                Collections.emptyMap(),
                getSourceTopic(connectionName), getTargetTopic(connectionName));

        final JsonObjectBuilder connectionJsonBuilder = connectionTemplate.toBuilder()
                .validateCertificate(true)
                .build()
                .toJson()
                .toBuilder();

        if (caCrt != null) {
            connectionJsonBuilder.set("ca", JsonFactory.newValue(caCrt));
        }
        connectionJsonBuilder.set("credentials", credentialsJson);

        return connectionJsonBuilder.build();
    }

    private static String getSourceTopic(final String connectionName) {
        return connectionName + "/source/#";
    }


    private static String getTargetTopic(final String connectionName) {
        return connectionName + "/target";
    }

    private static String getMqttUri(final ConnectivityTestConfig config) {
        return String.format("%s://%s:%d", "tcp", config.getMqttHostName(), config.getMqttPortTcp());
    }

    private static String getMqttInternalSslUri(final ConnectivityTestConfig config) {
        return String.format("%s://%s:%d", "ssl", config.getMqttHostName(), config.getMqttPortSsl());
    }

    private static String getMqttInternalTcpUri(final ConnectivityTestConfig config) {
        return String.format("%s://%s:%d", "tcp", config.getMqttHostName(), config.getMqttPortTcp());
    }

    private static String getMqttTunnelUri(final ConnectivityTestConfig config) {
        return String.format("%s://%s:%d", "tcp", config.getMqttTunnel(), config.getMqttPortTcp());
    }

    private static Enforcement getTopicEnforcement(final String topicFilter) {
        return ConnectivityModelFactory.newSourceAddressEnforcement(topicFilter);
    }

    private static String getTopicFilter(final String connectionName) {
        return connectionName + "/{{entity:id}}/source";
    }

}
