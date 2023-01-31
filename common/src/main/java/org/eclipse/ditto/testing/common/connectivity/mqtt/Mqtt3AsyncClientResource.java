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
package org.eclipse.ditto.testing.common.connectivity.mqtt;

import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.testing.common.junit.ExternalResourcePlus;
import org.eclipse.ditto.testing.common.junit.ExternalResourceState;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.message.connect.Mqtt3Connect;

/**
 * This external resource provides a {@link com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient}.
 * The client gets connected when a test is run and gets closed when the test finished.
 */
@NotThreadSafe
public final class Mqtt3AsyncClientResource extends ExternalResourcePlus {

    private static final Logger LOGGER = LoggerFactory.getLogger(Mqtt3AsyncClientResource.class);

    private final URI mqttEndpointUri;
    private final Duration clientOperationTimeout;
    private final CharSequence clientId;

    private ExternalResourceState state;
    private com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient mqtt3AsyncClient;

    private Mqtt3AsyncClientResource(final MqttConfig mqttConfig,
            final Duration clientOperationTimeout,
            final CharSequence clientId) {

        mqttEndpointUri = mqttConfig.getTcpUri();
        this.clientOperationTimeout = clientOperationTimeout;
        this.clientId = clientId;

        state = ExternalResourceState.INITIAL;
        mqtt3AsyncClient = null;
    }

    /**
     * Returns a new instance of {@code Mqtt3AsyncClientResource}.
     * The returned resource's client has a randomly generated ID consisting of eight alphabetic characters.
     *
     * @param mqttConfig the configuration settings to be used for the returned resource's client.
     * @param connectOperationTimeout defines how long to wait for the client to connect to the server.
     * @return the instance.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public static Mqtt3AsyncClientResource newInstanceWithRandomClientId(final MqttConfig mqttConfig,
            final Duration connectOperationTimeout) {

        return new Mqtt3AsyncClientResource(ConditionChecker.checkNotNull(mqttConfig, "mqttConfig"),
                ConditionChecker.checkNotNull(connectOperationTimeout, "connectOperationTimeout"),
                RandomStringUtils.randomAlphabetic(8));
    }

    @Override
    protected void before(final Description description) throws Throwable {
        super.before(description);
        mqtt3AsyncClient = instantiateMqtt3AsyncClient();
        mqtt3AsyncClient.connect(Mqtt3Connect.builder().build())
                .toCompletableFuture()
                .get(clientOperationTimeout.toMillis(), TimeUnit.MILLISECONDS);
        state = ExternalResourceState.POST_BEFORE;
    }

    private Mqtt3AsyncClient instantiateMqtt3AsyncClient() {
        return Mqtt3Client.builder()
                .serverAddress(new InetSocketAddress(mqttEndpointUri.getHost(), mqttEndpointUri.getPort()))
                .identifier(clientId.toString())
                .buildAsync();
    }

    @Override
    protected void after(final Description description) {
        try {
            mqtt3AsyncClient.disconnect();
        } catch (final Exception e) {
            LOGGER.error("Failed to close client <{}>: {}", getClientId(), e.getMessage(), e);
        } finally {
            state = ExternalResourceState.POST_AFTER;
            mqtt3AsyncClient = null;
        }
        super.after(description);
    }

    public com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient getClient() {
        if (state == ExternalResourceState.POST_BEFORE) {
            return mqtt3AsyncClient;
        } else if (state == ExternalResourceState.INITIAL) {
            throw new IllegalStateException("The async client gets only available with running a test.");
        } else {
            throw new IllegalStateException("The async client was already closed and cannot be used anymore.");
        }
    }

    public String getClientId() {
        return (String) clientId;
    }

}
