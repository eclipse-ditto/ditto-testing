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
package org.eclipse.ditto.testing.system.client.util;

import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.client.DisconnectedDittoClient;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.DittoClients;
import org.eclipse.ditto.client.configuration.MessagingConfiguration;
import org.eclipse.ditto.client.configuration.ProxyConfiguration;
import org.eclipse.ditto.client.configuration.WebSocketMessagingConfiguration;
import org.eclipse.ditto.client.live.internal.MessageSerializerFactory;
import org.eclipse.ditto.client.live.messages.MessageSerializer;
import org.eclipse.ditto.client.live.messages.MessageSerializerRegistry;
import org.eclipse.ditto.client.live.messages.MessageSerializers;
import org.eclipse.ditto.client.messaging.AuthenticationProvider;
import org.eclipse.ditto.client.messaging.MessagingProvider;
import org.eclipse.ditto.client.messaging.MessagingProviders;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.neovisionaries.ws.client.WebSocket;

/**
 * A factory for clients.
 */
public final class ClientFactory {

    public static final String DURATION_CONTENT_TYPE = "application/vnd.ditto-com.duration+xml";

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientFactory.class);

    private ClientFactory() {
        throw new AssertionError();
    }

    /**
     * Returns a new client for the specified parameters.
     *
     * @param endpoint the endpoint of the message service.
     * @param apiVersion the version of the API.
     * @param authenticationProvider authentication.
     * @param proxyConfig optional proxy configuration.
     * @param declaredTwinAcknowledgements acknowledgements that can be sent by the twin messaging provider
     * @param declaredLiveAcknowledgements acknowledgements that can be sent by the live messaging provider
     * @return the client.
     */
    public static DittoClient newClient(final String endpoint,
            final JsonSchemaVersion apiVersion,
            final AuthenticationProvider<WebSocket> authenticationProvider,
            @Nullable final ProxyConfiguration proxyConfig,
            final Collection<AcknowledgementLabel> declaredTwinAcknowledgements,
            final Collection<AcknowledgementLabel> declaredLiveAcknowledgements,
            @Nullable final Consumer<Throwable> errorHandler) {

        final MessagingProvider twinConfiguration =
                newTwinMessagingProvider(endpoint, apiVersion, authenticationProvider, proxyConfig,
                        declaredTwinAcknowledgements, errorHandler);

        final MessagingProvider liveConfiguration =
                newLiveMessagingProvider(endpoint, apiVersion, authenticationProvider, proxyConfig,
                        declaredLiveAcknowledgements);

        final String sessionId = authenticationProvider.getConfiguration().getSessionId();
        LOGGER.info("Connecting client <{}> to <{}> using API version <{}>", sessionId, endpoint, apiVersion);
        final DisconnectedDittoClient client = createClient(twinConfiguration, liveConfiguration);

        final DittoClient connectedClient = client.connect().toCompletableFuture().join();
        LOGGER.info("Init Things Client <{}> finished.", sessionId);
        return connectedClient;
    }

    /**
     * Returns a new client with the given {@code sharedConfig}.
     *
     * @param messagingProvider the shared messaging provider.
     * @return the client.
     */
    public static DisconnectedDittoClient newDisconnectedClient(final MessagingProvider messagingProvider) {
        LOGGER.info("Connecting client using shared messaging provider.");
        return createClient(messagingProvider, messagingProvider);
    }

    /**
     * Gets Twin messaging provider.
     *
     * @param endpointUri endpoint URI
     * @param apiVersion the version of the API.
     * @param authenticationProvider authentication.
     * @param proxyConfig optional proxy config.
     * @param declaredAcknowledgements acknowledgements that can be sent by this messaging provider
     * @return new IntegrationClientConfiguration instance
     */
    public static MessagingProvider newTwinMessagingProvider(final String endpointUri,
            final JsonSchemaVersion apiVersion,
            final AuthenticationProvider<WebSocket> authenticationProvider,
            @Nullable final ProxyConfiguration proxyConfig,
            final Collection<AcknowledgementLabel> declaredAcknowledgements,
            @Nullable final Consumer<Throwable> errorHandler) {
        return newTwinMessagingProvider(endpointUri, apiVersion, errorHandler != null, authenticationProvider,
                proxyConfig, errorHandler,
                declaredAcknowledgements);
    }

    /**
     * Gets Twin messaging provider.
     *
     * @param endpointUri endpoint URI
     * @param apiVersion the version of the API.
     * @param reconnect reconnect enabled/disabled.
     * @param authenticationProvider authentication.
     * @param proxyConfig optional proxy config.
     * @param declaredAcknowledgements acknowledgements that can be sent by this messaging provider
     * @return new IntegrationClientConfiguration instance
     */
    public static MessagingProvider newTwinMessagingProvider(final String endpointUri,
            final JsonSchemaVersion apiVersion,
            final boolean reconnect,
            final AuthenticationProvider<WebSocket> authenticationProvider,
            @Nullable final ProxyConfiguration proxyConfig,
            @Nullable final Consumer<Throwable> connectionErrorConsumer,
            final Collection<AcknowledgementLabel> declaredAcknowledgements) {
        return newMessagingProvider(endpointUri, apiVersion, reconnect, authenticationProvider, proxyConfig,
                connectionErrorConsumer, declaredAcknowledgements);
    }

    /**
     * Gets Twin messaging provider.
     *
     * @param endpointUri endpoint URI
     * @param apiVersion the version of the API.
     * @param authenticationProvider authentication.
     * @param proxyConfig optional proxy config.
     * @param declaredAcknowledgements acknowledgements that can be sent by this messaging provider
     * @return new IntegrationClientConfiguration instance
     */
    public static MessagingProvider newLiveMessagingProvider(final String endpointUri,
            final JsonSchemaVersion apiVersion,
            final AuthenticationProvider<WebSocket> authenticationProvider,
            @Nullable final ProxyConfiguration proxyConfig,
            final Collection<AcknowledgementLabel> declaredAcknowledgements) {
        return newLiveMessagingProvider(endpointUri, apiVersion, true, authenticationProvider, proxyConfig,
                declaredAcknowledgements);
    }

    /**
     * Gets Live messaging provider.
     *
     * @param endpointUri endpoint URI
     * @param apiVersion the version of the API.
     * @param reconnect reconnect enabled/disabled.
     * @param authenticationProvider authentication.
     * @param proxyConfig optional proxy config.
     * @param declaredAcknowledgements acknowledgements that can be sent by this messaging provider
     * @return new IntegrationClientConfiguration instance
     */
    public static MessagingProvider newLiveMessagingProvider(final String endpointUri,
            final JsonSchemaVersion apiVersion,
            final boolean reconnect,
            final AuthenticationProvider<WebSocket> authenticationProvider,
            @Nullable final ProxyConfiguration proxyConfig,
            final Collection<AcknowledgementLabel> declaredAcknowledgements) {

        return newMessagingProvider(endpointUri, apiVersion, reconnect, authenticationProvider, proxyConfig, null,
                declaredAcknowledgements);
    }

    private static ByteBuffer serializeDuration(final Duration duration, final Charset charset) {
        final String durationString = "<duration>" + duration.toString() + "</duration>";
        return ByteBuffer.wrap(durationString.getBytes(charset));
    }

    private static Duration tryToParseDurationString(final ByteBuffer byteBuffer, final Charset charset) {
        try {
            return parseDurationString(byteBuffer, charset);
        } catch (final DateTimeParseException e) {
            LOGGER.error("Could not parse date: {} - parsed String: {} - error index: {}",
                    e.getMessage(), e.getParsedString(), e.getErrorIndex(), e);
            throw e;
        }
    }

    private static Duration parseDurationString(final ByteBuffer byteBuffer, final Charset charset) {
        final DurationStringParser durationStringParser = DurationStringParser.getInstance(byteBuffer, charset);
        return durationStringParser.get();
    }

    public static MessagingProvider newMessagingProvider(final String endpointUri,
            final JsonSchemaVersion apiVersion,
            final boolean reconnect,
            final AuthenticationProvider<WebSocket> authenticationProvider,
            @Nullable final ProxyConfiguration proxyConfig,
            @Nullable final Consumer<Throwable> connectionErrorConsumer) {
        return newMessagingProvider(endpointUri, apiVersion, reconnect, authenticationProvider, proxyConfig,
                connectionErrorConsumer, Collections.emptyList());
    }

    public static MessagingProvider newMessagingProvider(final String endpointUri,
            final JsonSchemaVersion apiVersion,
            final boolean reconnect,
            final AuthenticationProvider<WebSocket> authenticationProvider,
            @Nullable final ProxyConfiguration proxyConfig,
            @Nullable final Consumer<Throwable> connectionErrorConsumer,
            final Collection<AcknowledgementLabel> declaredAcknowledgements) {
        requireNonNull(endpointUri, "endpointUri");

        final MessagingConfiguration.Builder messagingConfigurationBuilder =
                WebSocketMessagingConfiguration.newBuilder()
                        .endpoint(endpointUri)
                        .declaredAcknowledgements(declaredAcknowledgements)
                        .reconnectEnabled(reconnect)
                        .initialConnectRetryEnabled(reconnect)
                        .jsonSchemaVersion(apiVersion)
                        .connectionErrorHandler(connectionErrorConsumer);
        if (null != proxyConfig) {
            messagingConfigurationBuilder.proxyConfiguration(proxyConfig);
        }
        final MessagingConfiguration messagingConfiguration = messagingConfigurationBuilder.build();

        final MessagingProvider provider =
                MessagingProviders.webSocket(messagingConfiguration, authenticationProvider);

        IntegrationTest.rememberForCleanUp(() -> {
            LOGGER.info("Closing Websocket <{}>", authenticationProvider.getConfiguration().getSessionId());
            provider.close();
        });

        return provider;
    }

    private static DisconnectedDittoClient createClient(final MessagingProvider twinMessagingProvider,
            final MessagingProvider liveMessagingProvider) {
        final MessageSerializer<Duration> durationMessageSerializer = MessageSerializers.of(DURATION_CONTENT_TYPE,
                Duration.class,
                "*",
                ClientFactory::serializeDuration,
                ClientFactory::tryToParseDurationString);
        final MessageSerializerRegistry messageSerializerRegistry =
                MessageSerializerFactory.initializeDefaultSerializerRegistry();
        messageSerializerRegistry.registerMessageSerializer(durationMessageSerializer);
        final DisconnectedDittoClient client =
                DittoClients.newInstance(twinMessagingProvider, liveMessagingProvider, twinMessagingProvider,
                        messageSerializerRegistry);
        IntegrationTest.rememberForCleanUp(() -> {
            LOGGER.info("Destroying client <{}>",
                    twinMessagingProvider.getAuthenticationConfiguration().getSessionId());
            client.destroy();
        });
        return client;
    }

}
