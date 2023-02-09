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
package org.eclipse.ditto.testing.system.connectivity.amqp10;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;

import java.time.Duration;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.qpid.jms.JmsConnection;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.ConnectivityStatus;
import org.eclipse.ditto.connectivity.model.Enforcement;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.Resources;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectType;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.common.matcher.HttpVerbMatcher;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityITCommon;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityWorker;
import org.eclipse.ditto.testing.system.connectivity.ConnectionCategory;
import org.eclipse.ditto.testing.system.connectivity.ConnectionModelFactory;
import org.eclipse.ditto.testing.system.connectivity.Connections;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityConstants;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.eclipse.ditto.things.model.signals.events.ThingCreated;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.response.Response;

/**
 * Integration tests for Connectivity of AMQP1.0 connections against
 * a low-performance AmqpServer in the same JVM capable of emulating Eclipse Hono behavior.
 */
@RunIf(DockerEnvironment.class)
public class Amqp10HonoConnectivityIT extends AbstractConnectivityITCommon<BlockingQueue<Message>, Message> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Amqp10HonoConnectivityIT.class);
    private static final long WAIT_TIMEOUT_MS = 10_000L;

    private static final String AMQP10_HONO_HOSTNAME = CONFIG.getAmqp10HonoHostName();
    private static final int AMQP10_HONO_PORT = CONFIG.getAmqp10HonoPort();
    private static final ConnectionType AMQP10_TYPE = ConnectionType.AMQP_10;
    private static final Enforcement AMQP_ENFORCEMENT =
            ConnectivityModelFactory.newEnforcement("{{ header:device_id }}",
                    "{{ thing:namespace }}:{{thing:id | fn:substring-after(':')}}",
                    "{{ policy:id }}"
            );

    // refreshed for each test
    private static String sourceAddress;
    private static String targetAddress;

    private final Amqp10ConnectivityWorker connectivityWorker;

    private String honoConnectionName;

    private String targetAddressForTargetPlaceholderSubstitution;

    private Amqp10TestServer amqp10Server;

    private static final ConcurrentMap<String, MessageProducer> jmsSenders = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<String, MessageConsumer> jmsReceivers = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<String, JmsConnection> jmsConnections = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<String, Session> jmsSessions = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<String, MessageListener> jmsMessageListeners = new ConcurrentSkipListMap<>();

    public Amqp10HonoConnectivityIT() {
        super(ConnectivityFactory.of(
                "AmqpHono",
                connectionModelFactory,
                AMQP10_TYPE,
                Amqp10HonoConnectivityIT::getAmqpUri,
                () -> Collections.singletonMap("jms.closeTimeout", "0"),
                Amqp10HonoConnectivityIT::defaultTargetAddress,
                Amqp10HonoConnectivityIT::defaultSourceAddress,
                id -> AMQP_ENFORCEMENT,
                () -> SSH_TUNNEL_CONFIG));
        connectivityWorker = new Amqp10ConnectivityWorker(LOGGER,
                () -> jmsMessageListeners,
                () -> jmsSenders,
                () -> jmsReceivers,
                () -> jmsSessions,
                Amqp10HonoConnectivityIT::defaultTargetAddress,
                Amqp10HonoConnectivityIT::defaultSourceAddress,
                java.time.Duration.ofMillis(WAIT_TIMEOUT_MS));
    }

    @Override
    protected AbstractConnectivityWorker<BlockingQueue<Message>, Message> getConnectivityWorker() {
        return connectivityWorker;
    }

    @Before
    @Override
    public void setupConnectivity() throws Exception {
        super.setupConnectivity();
        honoConnectionName = UUID.randomUUID().toString();
        sourceAddress = cf.disambiguate("testSource");
        targetAddress = cf.disambiguate("testTarget");
        targetAddressForTargetPlaceholderSubstitution =
                String.format("%s%s", defaultTargetAddress(cf.connectionNameWithAuthPlaceholderOnHEADER_ID),
                        ConnectivityConstants.TARGET_SUFFIX);

        amqp10Server = new Amqp10TestServer(AMQP10_HONO_PORT);
        amqp10Server.startServer();

        LOGGER.info("Creating connections..");
        cf.setUpConnections(connectionsWatcher.getConnections());

        for (final String connectionName :
                cf.allConnectionNames(targetAddressForTargetPlaceholderSubstitution, honoConnectionName)) {
            connectAsClient(connectionName);
        }
    }

    @After
    public void cleanupConnectivity() throws Exception {
        try {
            jmsMessageListeners.clear();
            jmsSenders.clear();
            jmsReceivers.clear();
            jmsSessions.clear();
            jmsConnections.forEach((k, jmsConnection) -> {
                try {
                    jmsConnection.close();
                } catch (final JMSException e) {
                    LOGGER.error("Error during closing connection", e);
                }
            });
            jmsConnections.clear();
            cleanupConnections(testingContextWithRandomNs.getSolution().getUsername());
        } finally {
            amqp10Server.stopServer();
            amqp10Server = null;
        }
    }

    @Test
    @Connections(ConnectionCategory.CONNECTION2)
    public void testConnectionWithInvalidSourceAddress() throws Exception {

        // instruct the amqp 1.0 test server to simulate unknown sources link
        amqp10Server.enableHonoBehaviour(honoConnectionName);

        cf.setupSingleConnection(honoConnectionName).get();

        // execute simple test to verify connection is working
        sendSingleCommandAndEnsureEventIsProduced(honoConnectionName, cf.connectionName2);

        // stop "Hono"
        amqp10Server.stopProtonServer()
                .thenAccept(unused -> LOGGER.info("AMQP1.0 server is stopped."))
                .get(WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        // and restart again
        amqp10Server.startServer();

        // restart required test clients
        connectAsClient(cf.connectionName2);
        connectAsClient(honoConnectionName);

        // wait at most one minute until connection is established again
        Awaitility.await()
                .atMost(Duration.ofMinutes(1))
                .pollInterval(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    final Connection connection = getConnectionExistingByName(honoConnectionName);
                    final ConnectivityStatus connectionStatus = connection.getConnectionStatus();
                    LOGGER.info("ConnectionStatus: {}", connectionStatus);
                    assertThat(connectionStatus.getName()).isEqualTo(ConnectivityStatus.OPEN.getName());
                });

        // execute simple test to verify connection is working
        sendSingleCommandAndEnsureEventIsProduced(honoConnectionName, cf.connectionName2);
    }

    /**
     * Verifies using a target address with placeholders allows the user to send messages for edge
     * devices to the gateway device instead of the edge device. This is needed since the edge devices in this scenario
     * aren't registered in Hono but only known to the gateway.
     */
    @Test
    @Connections(ConnectionCategory.CONNECTION2)
    public void messageToEdgeDeviceIsSentToGatewayDevice() throws Exception {
        // instruct the amqp 1.0 test server to simulate unknown sources link
        amqp10Server.enableHonoBehaviour(honoConnectionName);

        setupSingleConnectionWithGatewayTarget(honoConnectionName).get();

        sendSingleCommandAndEnsureEventIsReceivedOnGatewayTarget(cf.connectionName2, honoConnectionName);
    }

    private void sendSingleCommandAndEnsureEventIsReceivedOnGatewayTarget(final String sendingConnection,
            final String receivingConnection) {
        // Given
        final ThingId gatewayId = generateThingId();
        final ThingId thingId = ThingId.of(gatewayId.getNamespace(), gatewayId.getName() + ":theThing");
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(connectionSubject(sendingConnection))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .forLabel("RESTRICTED")
                .setSubject(connectionSubject(receivingConnection))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ)
                .build();
        final String correlationId = createNewCorrelationId();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .correlationId(correlationId)
                .build();
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), dittoHeaders);

        // When
        final String gatewayTargetAddress = targetAddressWithGatewayTarget(receivingConnection, gatewayId.toString());
        final BlockingQueue<Message> eventConsumer =
                initTargetsConsumer(receivingConnection, gatewayTargetAddress);
        sendSignal(sendingConnection, createThing);

        // Then
        consumeAndAssert(receivingConnection, thingId, correlationId, eventConsumer, ThingCreated.class,
                tc -> assertThat(tc.getRevision()).isEqualTo(1L));
    }

    private String targetAddressWithGatewayTarget(final String connectionName, final String gatewayId) {
        return String.format("%s/%s", connectionName, gatewayId);
    }

    @Test
    @Connections(ConnectionCategory.CONNECTION_WITH_MULTIPLE_PAYLOAD_MAPPINGS)
    public void validateImplicitThingCreation() {
        final ThingId thingId = generateThingId();
        rememberForCleanUp(deleteThing(2, thingId));
        rememberForCleanUpLast(deletePolicy(thingId));
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .putHeader("tenant_id", "")
                .putHeader("device_id", thingId)
                .putHeader("gateway_id", "gateway:id")
                .putHeader("hono_registration_status", "NEW")
                .build();

        // send the "empty-notification" to trigger the implicit thing creation
        final String sourceAddress = cf.connectionNameWithMultiplePayloadMappings +
                ConnectionModelFactory.IMPLICIT_THING_CREATION_SOURCE_SUFFIX;
        final String correlationId = UUID.randomUUID().toString();

        connectivityWorker.sendWithEmptyBytePayload(sourceAddress, correlationId, dittoHeaders);

        // get the thing via http-api
        final Response response = getThing(2, thingId)
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.OK)
                .useAwaitility(Awaitility.await().atMost(Duration.ofSeconds(10)))
                .withLogging(LOGGER, "thing")
                .fire();
        final Thing thing = ThingsModelFactory.newThing(response.getBody().asString());

        assertThat(thing.getEntityId().orElseThrow().toString()).hasToString(thingId.toString());
        assertThat(thing.getPolicyId().orElseThrow().toString()).hasToString(thingId.toString());
    }

    @Test
    @Connections(ConnectionCategory.CONNECTION_WITH_MULTIPLE_PAYLOAD_MAPPINGS)
    public void validateImplicitStandaloneThingCreation() {
        final ThingId thingId = generateThingId();
        rememberForCleanUp(deleteThing(2, thingId));
        rememberForCleanUpLast(deletePolicy(thingId));
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .putHeader("tenant_id", "")
                .putHeader("device_id", thingId)
                .putHeader("hono_registration_status", "NEW")
                .build();

        // send the "empty-notification" to trigger the implicit thing creation
        final String sourceAddress = cf.connectionNameWithMultiplePayloadMappings +
                ConnectionModelFactory.IMPLICIT_THING_CREATION_SOURCE_SUFFIX;
        final String correlationId = UUID.randomUUID().toString();

        connectivityWorker.sendWithEmptyBytePayload(sourceAddress, correlationId, dittoHeaders);

        // get the thing via http-api
        final Response response = getThing(2, thingId)
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.OK)
                .useAwaitility(Awaitility.await().atMost(Duration.ofSeconds(10)))
                .withLogging(LOGGER, "thing")
                .fire();
        final Thing thing = ThingsModelFactory.newThing(response.getBody().asString());

        assertThat(thing.getEntityId().orElseThrow().toString()).hasToString(thingId.toString());
        assertThat(thing.getPolicyId().orElseThrow().toString()).hasToString(thingId.toString());
        final Policy policy = PoliciesModelFactory.newPolicy(getPolicy(thingId)
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .expectingStatusCode(HttpVerbMatcher.is(HttpStatus.OK))
                .fire().body().asString());

        final var username = testingContextWithRandomNs.getSolution().getUsername();
        final PolicyEntry expectedDeviceManagementEntry = PoliciesModelFactory.newPolicyEntry("DEVICE-MANAGEMENT",
                Subjects.newInstance(
                        Subject.newInstance("integration:" + username + ":iot-manager",
                                SubjectType.newInstance("ditto-integration"))
                ),
                Resources.newInstance(
                        Resource.newInstance(
                                PoliciesModelFactory.newResourceKey(PoliciesResourceType.policyResource("/")),
                                EffectedPermissions.newInstance(List.of("READ", "WRITE"), List.of())),
                        Resource.newInstance(
                                PoliciesModelFactory.newResourceKey(PoliciesResourceType.thingResource("/")),
                                EffectedPermissions.newInstance(List.of("READ", "WRITE"), List.of())),
                        Resource.newInstance(
                                PoliciesModelFactory.newResourceKey(PoliciesResourceType.messageResource("/")),
                                EffectedPermissions.newInstance(List.of("READ", "WRITE"), List.of()))
                )
        );

        assertThat(policy.getEntryFor(expectedDeviceManagementEntry.getLabel()))
                .contains(expectedDeviceManagementEntry);
    }

    private void connectAsClient(final String connectionName) throws NamingException, JMSException {
        connectAsClient(connectionName, defaultTargetAddress(connectionName));
    }

    private void connectAsClient(final String connectionName, final String targetAddress)
            throws JMSException, NamingException {

        final String uri = "amqp://" + AMQP10_HONO_HOSTNAME + ":" + AMQP10_HONO_PORT;

        final Context ctx = createContext(connectionName, uri);
        final JmsConnectionFactory cf = (JmsConnectionFactory) ctx.lookup(connectionName);

        @SuppressWarnings("squid:S2095") final JmsConnection jmsConnection = (JmsConnection) cf.createConnection();
        jmsConnection.start();

        @SuppressWarnings("squid:S2095") final Session jmsSession =
                jmsConnection.createSession(Session.AUTO_ACKNOWLEDGE);

        jmsConnections.put(connectionName, jmsConnection);
        jmsSessions.put(connectionName, jmsSession);

        final MessageConsumer receiver = connectivityWorker.createReceiver(connectionName, targetAddress);
        jmsReceivers.compute(targetAddress, (k, otherReceiver) -> {
            if (otherReceiver != null) {
                try {
                    otherReceiver.close();
                } catch (final JMSException e) {
                    LOGGER.error("Error during closing receiver", e);
                }
            }
            return receiver;
        });

        final MessageProducer sender =
                connectivityWorker.createSender(connectionName, defaultSourceAddress(connectionName));
        jmsSenders.compute(connectionName, (k, otherSender) -> {
            if (otherSender != null) {
                try {
                    otherSender.close();
                } catch (final JMSException e) {
                    LOGGER.error("Error during closing sender", e);
                }
            }
            return sender;
        });
    }

    private Context createContext(final String connectionName, final String connectionUri) throws NamingException {
        @SuppressWarnings("squid:S1149") final Hashtable<Object, Object> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        env.put("connectionfactory." + connectionName, connectionUri);

        return new InitialContext(env);
    }

    private static String defaultSourceAddress(final String suffix) {
        return sourceAddress + suffix;
    }

    private static String defaultTargetAddress(final String suffix) {
        return targetAddress + suffix;
    }

    private CompletableFuture<Response> setupSingleConnectionWithGatewayTarget(final String connectionName) {
        LOGGER.info("Creating an AMQP1.0 connection with name <{}> to <{}:{}> in Ditto Connectivity",
                connectionName, AMQP10_HONO_HOSTNAME, AMQP10_HONO_PORT);
        final Connection connection = connectionModelFactory.buildConnectionModel(
                testingContextWithRandomNs.getSolution().getUsername(),
                connectionName,
                AMQP10_TYPE,
                getAmqpUri(false, true),
                Collections.emptyMap(),
                defaultSourceAddress(connectionName),
                targetAddressWithGatewayTargetPlaceholder(connectionName));

        return cf.asyncCreateConnection(connection);
    }

    private String targetAddressWithGatewayTargetPlaceholder(final String connectionName) {
        return targetAddressWithGatewayTarget(connectionName,
                "{{ thing:namespace }}:{{ thing:name | fn:substring-before(':') | fn:default(thing:name) }}");
    }

    @Override
    protected String targetAddressForTargetPlaceHolderSubstitution() {
        return targetAddressForTargetPlaceholderSubstitution;
    }

    private static String getAmqpUri(final boolean tunnel, final boolean basicAuth) {
        final String host = tunnel ? CONFIG.getAmqp10HonoTunnel() : CONFIG.getAmqp10HonoHostName();
        return "amqp://" + host + ":" + CONFIG.getAmqp10HonoPort();
    }
}
