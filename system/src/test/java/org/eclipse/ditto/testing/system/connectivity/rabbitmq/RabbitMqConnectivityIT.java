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
package org.eclipse.ditto.testing.system.connectivity.rabbitmq;

import static org.eclipse.ditto.testing.system.connectivity.ConnectionModelFactory.DITTO_STATUS_SOURCE_SUFFIX;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionModelFactory.IMPLICIT_THING_CREATION_SOURCE_SUFFIX;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionModelFactory.JS_SOURCE_SUFFIX;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionModelFactory.MERGE_SOURCE_SUFFIX;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionModelFactory.SOURCE1_SUFFIX;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionModelFactory.SOURCE2_SUFFIX;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionModelFactory.STATUS_SOURCE_SUFFIX;
import static org.eclipse.ditto.testing.system.connectivity.ConnectivityConstants.TARGET_SUFFIX;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.Enforcement;
import org.eclipse.ditto.connectivity.model.HeaderMapping;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityITestCases;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityWorker;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * Integration tests for Connectivity of RabbitMQ connections.
 */
@RunIf(DockerEnvironment.class)
@NotThreadSafe
public class RabbitMqConnectivityIT extends
        AbstractConnectivityITestCases<QueueingConsumer, QueueingConsumer.Delivery> {

    private static final HeaderMapping DEFAULT_HEADER_MAPPING =
            ConnectivityModelFactory.newHeaderMapping(JsonObject.newBuilder()
                    .set("correlation-id", "{{header:correlation-id}}")
                    .set("content-type", "{{header:content-type}}")
                    .set("reply-to", "{{header:reply-to}}")
                    .build());

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMqConnectivityIT.class);

    private static final String INBOUND_EXCHANGE = "inboundExchange";
    private static final String SOURCE_ADDRESS = "inboundThingCommandQueue";

    private static final String OUTBOUND_EXCHANGE = "outboundExchange";
    private static final String OUTBOUND_ROUTING_KEY = "outboundRoutingKey";
    private static final String TARGET_ADDRESS = OUTBOUND_EXCHANGE + "/" + OUTBOUND_ROUTING_KEY;

    private static final ConnectionFactory RABBIT_CONNECTION_FACTORY = createRabbitConnectionFactory();

    private static final ConnectionType AMQP_091_TYPE = ConnectionType.AMQP_091;
    private static final Enforcement RABBITMQ_ENFORCEMENT = ConnectivityModelFactory.newEnforcement(
            "{{ header:very_unknown_header | fn:default(header:device_id) }}", "{{ entity:id }}"
    );

    private static Channel channel;
    private static final List<String> declaredQueues = new ArrayList();

    @Rule
    public final TestWatcher watchman = new DefaultTestWatcher(LOGGER);

    private final RabbitMqConnectivityWorker rabbitConnectivityWorker;

    public RabbitMqConnectivityIT() {
        super(ConnectivityFactory.of(
                "RabbitCon",
                connectionModelFactory,
                () -> SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution(),
                AMQP_091_TYPE,
                RabbitMqConnectivityIT::getConnectionUri,
                Collections::emptyMap,
                TARGET_ADDRESS::concat,
                SOURCE_ADDRESS::concat,
                id -> RABBITMQ_ENFORCEMENT,
                () -> SSH_TUNNEL_CONFIG,
                SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient())
                .withDefaultHeaderMapping(DEFAULT_HEADER_MAPPING.getMapping())
                .withDefaultReplyTargetAddress("/{{header:reply-to}}"));
        rabbitConnectivityWorker =
                new RabbitMqConnectivityWorker(LOGGER, () -> channel, RabbitMqConnectivityIT::defaultTargetAddress,
                        Duration.ofSeconds(10), INBOUND_EXCHANGE, OUTBOUND_ROUTING_KEY, SOURCE_ADDRESS);
    }

    @Before
    public void setUpRabbit() throws Exception {
        LOGGER.info("Connecting to RabbitMQ at {}:{}",
                CONFIG.getRabbitMqHostname(),
                CONFIG.getRabbitMqPort());
        channel = connectToRabbitMQAndCreateChannel();
        declareExchangesAndQueues();
        cf.setUpConnections(connectionsWatcher.getConnections());
    }

    @After
    public void closeRabbitChannelAndDeleteConnections() {
        if (channel != null) {
            deleteQueues();

            try {
                channel.getConnection().close(5000 /* ms */);
            } catch (final Exception e) {
                LOGGER.debug("Close failed, but can be ignored ({}).", e.getMessage());
            }
        }
        cleanupConnections(SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution().getUsername());
    }

    private void deleteQueues() {
        declaredQueues.forEach(this::deleteQueue);
    }

    private void deleteQueue(final String queue) {
        try {
            channel.queueDelete(queue);
        } catch (IOException e) {
            LOGGER.debug("Deleting queue failed, but can be ignored({})", e.getMessage());
        }
    }

    private void declareExchangesAndQueues() throws Exception {
        channel.exchangeDeclare(INBOUND_EXCHANGE, "direct", false, false, false, Map.of());
        channel.exchangeDeclare(OUTBOUND_EXCHANGE, "direct", false, false, false, Map.of());
        // bind standard queues
        for (final String connectionNameWithOptionalSuffix : cf.allConnectionNames(
                // these are special queues
                cf.connectionNameWithMultiplePayloadMappings + DITTO_STATUS_SOURCE_SUFFIX,
                cf.connectionNameWithMultiplePayloadMappings + STATUS_SOURCE_SUFFIX,
                cf.connectionNameWithMultiplePayloadMappings + IMPLICIT_THING_CREATION_SOURCE_SUFFIX,
                cf.connectionNameWithMultiplePayloadMappings + JS_SOURCE_SUFFIX,
                cf.connectionNameWithMultiplePayloadMappings + MERGE_SOURCE_SUFFIX,
                cf.connectionNameWithAuthPlaceholderOnHEADER_ID + TARGET_SUFFIX,
                cf.connectionWith2Sources + SOURCE1_SUFFIX,
                cf.connectionWith2Sources + SOURCE2_SUFFIX
        )) {
            final String inboundQueue = SOURCE_ADDRESS + connectionNameWithOptionalSuffix;
            final String outboundQueue = OUTBOUND_ROUTING_KEY + connectionNameWithOptionalSuffix;
            channel.queueDeclare(inboundQueue, false, false, false, Map.of());
            declaredQueues.add(inboundQueue);

            channel.queueDeclare(outboundQueue, false, false, false, Map.of());
            declaredQueues.add(outboundQueue);

            channel.queueBind(inboundQueue, INBOUND_EXCHANGE, inboundQueue);
            channel.queueBind(outboundQueue, OUTBOUND_EXCHANGE, outboundQueue);
        }
    }

    private static String defaultTargetAddress(final String connectionName) {
        return OUTBOUND_ROUTING_KEY + connectionName;
    }

    @Override
    protected String targetAddressForTargetPlaceHolderSubstitution() {
        return defaultTargetAddress(cf.connectionNameWithAuthPlaceholderOnHEADER_ID) + TARGET_SUFFIX;
    }

    @Override
    protected AbstractConnectivityWorker<QueueingConsumer, QueueingConsumer.Delivery> getConnectivityWorker() {
        return rabbitConnectivityWorker;
    }

    private static Channel connectToRabbitMQAndCreateChannel() {
        try {
            return RABBIT_CONNECTION_FACTORY.newConnection().createChannel();
        } catch (final IOException | TimeoutException e) {
            throw mapException(e);
        }
    }

    private static ConnectionFactory createRabbitConnectionFactory() {
        final ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(CONFIG.getRabbitMqHostname());
        connectionFactory.setPort(CONFIG.getRabbitMqPort());
        connectionFactory.setUsername(CONFIG.getRabbitMqUser());
        connectionFactory.setPassword(CONFIG.getRabbitMqPassword());
        return connectionFactory;
    }

    private static String getConnectionUri(final boolean tunnel, final boolean basicAuth) {
        final String host = tunnel ? CONFIG.getRabbitMqTunnel() : CONFIG.getRabbitMqHostname();

        final StringBuilder stringBuilder = new StringBuilder("amqp://");
        if (basicAuth) {
            stringBuilder.append(CONFIG.getRabbitMqUser()).append(":").append(CONFIG.getRabbitMqPassword()).append("@");
        }
        stringBuilder.append(host).append(":").append(CONFIG.getRabbitMqPort());
        return stringBuilder.toString();
    }
}
