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
import static org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2;
import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;

import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.IntStream;

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
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.exceptions.DittoRuntimeException;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.signals.GlobalErrorRegistry;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.Enforcement;
import org.eclipse.ditto.connectivity.model.MessageSendingFailedException;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityITCommon;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityWorker;
import org.eclipse.ditto.testing.system.connectivity.ConnectionCategory;
import org.eclipse.ditto.testing.system.connectivity.ConnectionModelFactory;
import org.eclipse.ditto.testing.system.connectivity.Connections;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.response.Response;

/**
 * Test congestion at AmqpPublisherActor.
 */
@RunIf(DockerEnvironment.class)
public class Amqp10PublisherCongestionIT extends AbstractConnectivityITCommon<BlockingQueue<Message>, Message> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Amqp10PublisherCongestionIT.class);
    private static final long WAIT_TIMEOUT_MS = 10_000L;

    private static final String AMQP10_HONO_HOSTNAME = CONFIG.getAmqp10HonoHostName();
    private static final int AMQP10_HONO_CONGESTION_PORT = CONFIG.getAmqp10HonoCongestionPort();
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

    private Amqp10TestServer amqp10Server;

    private static final ConcurrentMap<String, MessageProducer> jmsSenders = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<String, MessageConsumer> jmsReceivers = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<String, JmsConnection> jmsConnections = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<String, Session> jmsSessions = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<String, MessageListener> jmsMessageListeners = new ConcurrentSkipListMap<>();

    public Amqp10PublisherCongestionIT() {
        super(ConnectivityFactory.of(
                "AmqpHono",
                connectionModelFactory,
                AMQP10_TYPE,
                Amqp10PublisherCongestionIT::getAmqpUri,
                () -> Map.of("jms.closeTimeout", "0", "jms.sendTimeout", "120000"),
                Amqp10PublisherCongestionIT::defaultTargetAddress,
                Amqp10PublisherCongestionIT::defaultSourceAddress,
                id -> AMQP_ENFORCEMENT,
                () -> SSH_TUNNEL_CONFIG));
        connectivityWorker = new Amqp10ConnectivityWorker(LOGGER,
                () -> jmsMessageListeners,
                () -> jmsSenders,
                () -> jmsReceivers,
                () -> jmsSessions,
                Amqp10PublisherCongestionIT::defaultTargetAddress,
                Amqp10PublisherCongestionIT::defaultSourceAddress,
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
        sourceAddress = cf.disambiguate("testSource");
        targetAddress = cf.disambiguate("testTarget");

        amqp10Server = new Amqp10TestServer(AMQP10_HONO_CONGESTION_PORT, 0);
        amqp10Server.startServer();

        LOGGER.info("Creating connections..");
        cf.setUpConnections(connectionsWatcher.getConnections());

        for (final String connectionName : cf.allConnectionNames()) {
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
    @Connections({ConnectionCategory.CONNECTION1})
    public void dropMessagesToPublishIfBrokerDoesNotGiveCredits() {
        // GIVEN: AMQP server does not give connections any credit
        final ThingId thingId = generateThingId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(cf.connectionName1))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .build();

        putThingWithPolicy(2, thing, policy, V_2)
                .withConfiguredAuth(testingContextWithRandomNs)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        // WHEN: connection1 is flooded with enough events to fill the 100 element queue and 10 sending futures
        IntStream.range(0, 1010)
                .forEach(i -> putAttribute(2, thingId, "x", "5")
                        .withConfiguredAuth(testingContextWithRandomNs)
                        .expectingStatusCodeSuccessful()
                        .fire());

        // THEN: connection1 drops subsequent events
        final Response response = putAttribute(2, thingId, "x", "5")
                .withConfiguredAuth(testingContextWithRandomNs)
                .withHeader("requested-acks", ConnectionModelFactory.toAcknowledgementLabel(
                        cf.getConnectionId(cf.connectionName1),
                        defaultTargetAddress(cf.connectionName1))
                        .toString())
                .withHeader("timeout", "10s")
                .fire();

        final int expectedStatusCode = HttpStatus.SERVICE_UNAVAILABLE.getCode();
        assertThat(response.getStatusCode()).isEqualTo(expectedStatusCode);
        final DittoRuntimeException body = GlobalErrorRegistry.getInstance()
                .parse(JsonObject.of(response.body().asString()).toBuilder()
                                .set(DittoRuntimeException.JsonFields.STATUS, expectedStatusCode)
                                .build(),
                        DittoHeaders.empty());
        assertThat(body.getErrorCode()).isEqualTo(MessageSendingFailedException.ERROR_CODE);
        assertThat(body.getMessage()).contains("dropped");
    }

    private void connectAsClient(final String connectionName) throws NamingException, JMSException {
        connectAsClient(connectionName, defaultTargetAddress(connectionName));
    }

    private void connectAsClient(final String connectionName, final String targetAddress)
            throws JMSException, NamingException {

        final String uri = "amqp://" + AMQP10_HONO_HOSTNAME + ":" + AMQP10_HONO_CONGESTION_PORT;

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

    private static Context createContext(final String connectionName, final String connectionUri)
            throws NamingException {

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

    @Override
    protected String targetAddressForTargetPlaceHolderSubstitution() {
        return defaultTargetAddress("");
    }

    private static String getAmqpUri(final boolean tunnel, final boolean basicAuth) {
        final String host = tunnel ? CONFIG.getAmqp10HonoTunnel() : CONFIG.getAmqp10HonoHostName();
        return "amqp://" + host + ":" + AMQP10_HONO_CONGESTION_PORT;
    }
}
