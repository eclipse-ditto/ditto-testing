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

import java.time.Duration;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.annotation.concurrent.NotThreadSafe;
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
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.Enforcement;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityITestCases;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityWorker;
import org.eclipse.ditto.testing.system.connectivity.ConnectionCategory;
import org.eclipse.ditto.testing.system.connectivity.Connections;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityConstants;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for Connectivity of AMQP1.0 connections.
 */
@RunIf(DockerEnvironment.class)
@NotThreadSafe
public class Amqp10ConnectivityIT extends AbstractConnectivityITestCases<BlockingQueue<Message>, Message> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Amqp10ConnectivityIT.class);
    private static final long WAIT_TIMEOUT_MS = 10_000L;

    private static final String AMQP10_HOSTNAME = CONFIG.getAmqp10HostName();
    private static final int AMQP10_PORT = CONFIG.getAmqp10Port();
    private static final ConnectionType AMQP10_TYPE = ConnectionType.AMQP_10;
    private static final Enforcement AMQP_ENFORCEMENT =
            ConnectivityModelFactory.newEnforcement("{{ header:device_id }}",
                    "{{ thing:namespace }}:{{thing:id | fn:substring-after(':')}}",
                    "{{ policy:id }}"
            );

    private static final ConcurrentMap<String, MessageProducer> JMS_SENDERS = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<String, MessageConsumer> JMS_RECEIVERS = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<String, JmsConnection> JMS_CONNECTIONS = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<String, Session> JMS_SESSIONS = new ConcurrentSkipListMap<>();
    private static final ConcurrentMap<String, MessageListener> JMS_MESSAGE_LISTENERS = new ConcurrentSkipListMap<>();

    // refreshed for each test
    private static String sourceAddress;
    private static String targetAddress;

    private final Amqp10ConnectivityWorker connectivityWorker;

    private String targetAddressForTargetPlaceholderSubstitution;

    public Amqp10ConnectivityIT() {
        super(ConnectivityFactory.of(
                "Amqp",
                connectionModelFactory,
                AMQP10_TYPE,
                Amqp10ConnectivityIT::getAmqpUri,
                () -> Collections.singletonMap("jms.closeTimeout", "0"),
                Amqp10ConnectivityIT::defaultTargetAddress,
                Amqp10ConnectivityIT::defaultSourceAddress,
                id -> AMQP_ENFORCEMENT,
                () -> SSH_TUNNEL_CONFIG));

        connectivityWorker = new Amqp10ConnectivityWorker(LOGGER,
                () -> JMS_MESSAGE_LISTENERS,
                () -> JMS_SENDERS,
                () -> JMS_RECEIVERS,
                () -> JMS_SESSIONS,
                Amqp10ConnectivityIT::defaultTargetAddress,
                Amqp10ConnectivityIT::defaultSourceAddress,
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
        targetAddressForTargetPlaceholderSubstitution =
                String.format("%s%s", defaultTargetAddress(cf.connectionNameWithAuthPlaceholderOnHEADER_ID),
                        ConnectivityConstants.TARGET_SUFFIX);

        LOGGER.info("Creating connections..");
        cf.setUpConnections(connectionsWatcher.getConnections());

        for (final String connectionName : cf.allConnectionNames(targetAddressForTargetPlaceholderSubstitution)) {
            connectAsClient(connectionName);
        }
    }

    @After
    public void cleanupConnectivity() {
        JMS_MESSAGE_LISTENERS.clear();
        closeEntryValuesAndRemoveEntries(JMS_SENDERS);
        closeEntryValuesAndRemoveEntries(JMS_RECEIVERS);
        closeEntryValuesAndRemoveEntries(JMS_SESSIONS);
        closeEntryValuesAndRemoveEntries(JMS_CONNECTIONS);
        cleanupConnections(testingContextWithRandomNs.getSolution().getUsername());
    }

    private static <T extends AutoCloseable> void closeEntryValuesAndRemoveEntries(final Map<String, T> map) {
        final var entryIterator = map.entrySet().iterator();
        while (entryIterator.hasNext()) {
            final var entry = entryIterator.next();
            final var entryValue = entry.getValue();
            try {
                entryValue.close();
            } catch (final Exception e) {
                LOGGER.warn("Failed to close {} MessageProducer: {}",
                        entryValue.getClass().getSimpleName(),
                        e.getMessage());
            } finally {
                entryIterator.remove();
            }
        }
    }

    @Test
    @Connections(ConnectionCategory.CONNECTION1)
    public void sendManyThingModifyCommandsAndVerifyProcessingIsThrottled() {

        // tolerance for timing inaccuracy
        final double timingTolerance = 0.20;

        // maximum number of lost messages
        final int maxMessageLoss = 0;

        // consumer is configured to allow one command every 100ms -> see docker config.
        // we allow a tolerance of 25% here because we cannot handle too small delays correctly either, hence the
        // minimum average delay is 75 milliseconds
        final double expectedPerMessageDelayMs = 10;

        // create thing with READ permission for the default solution
        final ThingId thingId = sendCreateThingAndEnsureResponseIsSentBack(cf.connectionName1,
                ThingBuilder.FromScratch::build);

        final JsonPointer attributeKey = JsonPointer.of("test");

        final int numberOfMessages = 400;

        final double startTime = System.currentTimeMillis();

        // send 200 messages while waiting for acknowledgement - lest AMQP client crashes
        for (int i = 0; i < numberOfMessages; ++i) {
            final String connectionName = cf.connectionName1;
            final String attributeValue = "value " + i;
            sendSignal(connectionName, modifyAttribute(thingId, attributeKey, attributeValue));
        }
        final int expectedArrivals = numberOfMessages - maxMessageLoss;

        // expect all messages are processed
        getThing(2, thingId)
                .withParam("fields", "_revision")
                .withConfiguredAuth(testingContextWithRandomNs)
                .useAwaitility(Awaitility.await()
                        .pollInterval(Duration.ofSeconds(1))
                        .atMost(Duration.ofMinutes(1)))
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(satisfies(body ->
                        assertThat(JsonFactory.newObject(body).getValueOrThrow(Thing.JsonFields.REVISION))
                                .isGreaterThan(expectedArrivals)))
                .fire();

        final double endTime = System.currentTimeMillis();
        final double delay = endTime - startTime;
        final double expectedDelay = expectedArrivals * expectedPerMessageDelayMs;
        LOGGER.info("Total processing time of {} ModifyAttribute was {}ms (expected: {}ms)", expectedArrivals,
                delay, expectedDelay);

        assertThat(delay).isGreaterThan(expectedDelay * (1.0 - timingTolerance));
    }

    private void connectAsClient(final String connectionName) throws NamingException, JMSException {
        connectAsClient(connectionName, defaultTargetAddress(connectionName));
    }

    private void connectAsClient(final String connectionName, final String targetAddress)
            throws JMSException, NamingException {

        final String uri = "amqp://" + AMQP10_HOSTNAME + ":" + AMQP10_PORT;

        final Context ctx = createContext(connectionName, uri);
        final JmsConnectionFactory cf = (JmsConnectionFactory) ctx.lookup(connectionName);

        @SuppressWarnings("squid:S2095") final JmsConnection jmsConnection = (JmsConnection) cf.createConnection();
        jmsConnection.start();

        @SuppressWarnings("squid:S2095") final Session jmsSession =
                jmsConnection.createSession(Session.AUTO_ACKNOWLEDGE);

        JMS_CONNECTIONS.put(connectionName, jmsConnection);
        JMS_SESSIONS.put(connectionName, jmsSession);

        final MessageConsumer receiver = connectivityWorker.createReceiver(connectionName, targetAddress);
        JMS_RECEIVERS.compute(targetAddress, (k, otherReceiver) -> {
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
        JMS_SENDERS.compute(connectionName, (k, otherSender) -> {
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
        return targetAddressForTargetPlaceholderSubstitution;
    }

    private static String getAmqpUri(final boolean tunnel, final boolean basicAuth) {
        final String host = tunnel ? CONFIG.getAmqp10Tunnel() : CONFIG.getAmqp10HostName();
        return "amqp://" + host + ":" + CONFIG.getAmqp10Port();
    }

}
