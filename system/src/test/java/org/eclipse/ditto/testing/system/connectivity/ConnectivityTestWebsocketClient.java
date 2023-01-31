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

import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.netty.ws.NettyWebSocket;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;
import org.eclipse.ditto.base.model.exceptions.DittoRuntimeException;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.headers.translator.HeaderTranslator;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.commands.Command;
import org.eclipse.ditto.base.model.signals.commands.CommandResponse;
import org.eclipse.ditto.base.model.signals.events.Event;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonParseException;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.messages.model.signals.commands.MessageCommand;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.TopicPath;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
import org.eclipse.ditto.protocol.adapter.ProtocolAdapter;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for Things-WS.
 */
public final class ConnectivityTestWebsocketClient implements WebSocketListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectivityTestWebsocketClient.class);

    private static final String PROTOCOL_CMD_START_SEND_EVENTS = "START-SEND-EVENTS";
    private static final String PROTOCOL_CMD_START_SEND_MESSAGES = "START-SEND-MESSAGES";
    private static final String PROTOCOL_CMD_START_SEND_LIVE_COMMANDS = "START-SEND-LIVE-COMMANDS";
    private static final String PROTOCOL_CMD_STOP_SEND_EVENTS = "STOP-SEND-EVENTS";

    private static final long CONNECTION_TIMEOUT_MILLIS = 5000;

    private final DefaultAsyncHttpClient client;
    private final ProtocolAdapter protocolAdapter;
    private final String endpoint;
    private final String authToken;

    private final Map<String, PendingResponse> pendingResponses;

    private WebSocket webSocket;
    private Consumer<Event<?>> eventConsumer;
    private Consumer<MessageCommand<?, ?>> messageCommandConsumer;
    private Consumer<Command<?>> liveCommandConsumer;

    private final ConcurrentMap<String, CompletableFuture<Void>> pendingAcknowledgements =
            new ConcurrentSkipListMap<>();

    private ConnectivityTestWebsocketClient(final String endpoint, final String authToken) {
        final DefaultAsyncHttpClientConfig.Builder configBuilder = new DefaultAsyncHttpClientConfig.Builder();
        configBuilder.setThreadPoolName("things-ws");
        configBuilder.setUserAgent("ConnectivityTestWebsocketClient");
        configBuilder.setWebSocketMaxFrameSize(16 * 1024);

        this.client = new DefaultAsyncHttpClient(configBuilder.build());
        this.protocolAdapter = DittoProtocolAdapter.of(HeaderTranslator.empty());
        this.endpoint = endpoint;
        this.authToken = authToken;
        this.pendingResponses = new HashMap<>();
    }

    public static ConnectivityTestWebsocketClient newInstance(final String endpoint, final String authToken) {
        requireNonNull(endpoint);
        requireNonNull(authToken);
        return new ConnectivityTestWebsocketClient(endpoint, authToken);
    }

    /**
     * Connects the client.
     */
    public void connect(final String correlationId) {

        final WebSocketUpgradeHandler upgradeHandler = new WebSocketUpgradeHandler.Builder()
                .addWebSocketListener(this)
                .build();
        final ListenableFuture<NettyWebSocket> execute = client
                .prepareGet(endpoint)
                .addHeader(HttpHeader.AUTHORIZATION.getName(), "Bearer " + authToken)
                .addHeader(HttpHeader.X_CORRELATION_ID.getName(), correlationId)
                .execute(upgradeHandler);

        safeGet(execute);
    }

    /**
     * Disconnects to client.
     */
    public void disconnect() {
        webSocket = null;
        try {
            LOGGER.debug("Closing WebSocket client of endpoint '{}'", endpoint);
            client.close();
            LOGGER.debug("WebSocket client destroyed");
        } catch (final Exception e) {
            LOGGER.info("Exception occurred while trying to shutdown http client.", e);
        }
    }

    /**
     * Sends a {@link Signal}.
     *
     * @param signal the signal to send.
     * @return the response to the signal.
     */
    public CompletableFuture<CommandResponse<?>> send(final Signal<?> signal) {
        final CompletableFuture<CommandResponse<?>> responseFuture = new CompletableFuture<>();
        if (signal.getDittoHeaders().isResponseRequired() &&
                signal.getDittoHeaders().getCorrelationId().isPresent()) {
            pendingResponses.put(signal.getDittoHeaders().getCorrelationId().orElseThrow(),
                    new PendingResponse(responseFuture));
        }

        sendAdaptable(protocolAdapter.toAdaptable(signal));
        return responseFuture;
    }

    /**
     * Starts consuming {@link Event}s.
     *
     * @param eventConsumer a consumer for all incoming events.
     */
    public CompletableFuture<Void> startConsumingEvents(final Consumer<Event<?>> eventConsumer) {
        final String message = PROTOCOL_CMD_START_SEND_EVENTS;
        final CompletableFuture<Void> future = addPendingAcknowledgement(message);
        this.eventConsumer = eventConsumer;
        webSocket.sendTextFrame(message);
        return future;
    }

    /**
     * Starts consuming {@link MessageCommand}s.
     *
     * @param messageCommandConsumer a consumer for all incoming messages.
     */
    public CompletableFuture<Void> startConsumingMessages(final Consumer<MessageCommand<?, ?>> messageCommandConsumer) {
        final String message = PROTOCOL_CMD_START_SEND_MESSAGES;
        final CompletableFuture<Void> future = addPendingAcknowledgement(message);
        this.messageCommandConsumer = messageCommandConsumer;
        webSocket.sendTextFrame(message);
        return future;
    }

    /**
     * Starts consuming live {@link Command}s.
     *
     * @param liveCommandConsumer a consumer for all incoming live commands.
     */
    public CompletableFuture<Void> startConsumingLiveCommands(final Consumer<Command<?>> liveCommandConsumer) {
        final String message = PROTOCOL_CMD_START_SEND_LIVE_COMMANDS;
        final CompletableFuture<Void> future = addPendingAcknowledgement(message);
        this.liveCommandConsumer = liveCommandConsumer;
        webSocket.sendTextFrame(message);
        return future;
    }

    /**
     * Stops consuming {@link Event}s.
     */
    public CompletableFuture<Void> stopConsumingEvents() {
        final String message = PROTOCOL_CMD_STOP_SEND_EVENTS;
        final CompletableFuture<Void> future = addPendingAcknowledgement(message);
        this.eventConsumer = null;
        webSocket.sendTextFrame(message);
        return future;
    }

    @Override
    public void onBinaryFrame(final byte[] message, final boolean finalFragment, final int rsv) {
        final String stringMessage = new String(message, StandardCharsets.UTF_8);
        LOGGER.debug("Received webSocket byte array message '{}', as string '{}' - don't know what to do with it!.",
                message, stringMessage);
    }

    @Override
    public void onTextFrame(final String message, final boolean finalFragment, final int rsv) {
        LOGGER.debug("Received webSocket string message '{}'.", message);
        if (message.startsWith("START") || message.startsWith("STOP")) {
            LOGGER.info("Got: {}", message);
            pendingAcknowledgements.compute(message, (k, future) -> {
                if (future == null) {
                    throw new IllegalStateException(String.format("Got %s without pending ack future!", message));
                } else {
                    future.complete(null);
                    return null;
                }
            });
        } else {
            handleIncomingAdaptable(message);
        }
    }

    @Override
    public void onOpen(final WebSocket webSocket) {
        LOGGER.info("Connection to '{}' established.", endpoint);
        this.webSocket = webSocket;
    }

    @Override
    public void onClose(final WebSocket webSocket, final int code, final String reason) {
        LOGGER.info("Connection to '{}' closed by remote (code: {}, reason: {}).", endpoint, code, reason);
    }

    @Override
    public void onError(final Throwable throwable) {
        if (throwable instanceof DittoRuntimeException) {
            LOGGER.warn("Got DittoRuntimeException which should not happen during runtime: {}",
                    throwable.getMessage());
        } else {
            LOGGER.error("Error in WebSocket: {}", throwable != null ?
                    throwable.getClass().getName() + ": " + throwable.getMessage() : "-");
        }
    }

    private void sendAdaptable(final Adaptable adaptable) {

        if (webSocket != null) {
            final String stringMessage = ProtocolFactory.wrapAsJsonifiableAdaptable(adaptable).toJsonString();
            LOGGER.info("Sending JSON: {}", stringMessage);
            webSocket.sendTextFrame(stringMessage);
        } else {
            throw new IllegalStateException("WebSocket is not connected!");
        }
    }

    private void handleIncomingAdaptable(final String message) {
        final JsonObject messageJson;
        try {
            messageJson = Optional.of(JsonFactory.readFrom(message))
                    .filter(JsonValue::isObject)
                    .map(JsonValue::asObject)
                    .orElseThrow(() -> new JsonParseException(
                            "The Websocket message was not a JSON object as required:\n" + message));
        } catch (final JsonParseException e) {
            LOGGER.warn("Got unknown non-JSON message on websocket: {}", message, e);
            return;
        }

        final JsonifiableAdaptable jsonifiableAdaptable =
                ProtocolFactory.jsonifiableAdaptableFromJson(messageJson);
        final DittoHeaders headers = jsonifiableAdaptable.getDittoHeaders();
        final Optional<PendingResponse> pendingResponse = headers.getCorrelationId()
                .map(pendingResponses::remove);

        final Signal<?> signal = protocolAdapter.fromAdaptable(jsonifiableAdaptable);

        if (signal instanceof CommandResponse) {
            LOGGER.debug("Received Response JSON: {}", message);
            pendingResponse.ifPresent(pr -> pr.getResponseFuture().complete((CommandResponse<?>) signal));
        } else if (signal instanceof MessageCommand) {
            LOGGER.debug("Received MessageCommand JSON: {}", message);
            if (messageCommandConsumer != null) {
                messageCommandConsumer.accept((MessageCommand<?, ?>) signal);
            } else {
                LOGGER.debug(
                        "Dropping incoming MessageCommand as no subscription for consuming MessageCommands was registered. " +
                                "Did you call 'client.startConsumingMessageCommands()' ?");
            }
        } else if (signal instanceof Command &&
                signal.getDittoHeaders().getChannel().orElse(TopicPath.Channel.TWIN.getName())
                        .equals(TopicPath.Channel.LIVE.getName())) {
            LOGGER.debug("Received live Command JSON: {}", message);
            if (liveCommandConsumer != null) {
                liveCommandConsumer.accept((Command<?>) signal);
            } else {
                LOGGER.debug(
                        "Dropping incoming live Command as no subscription for consuming live Commands was registered. " +
                                "Did you call 'client.startConsumingLiveCommands()' ?");
            }
        } else if (signal instanceof Event) {
            LOGGER.debug("Received Event JSON: {}", message);
            if (eventConsumer != null) {
                eventConsumer.accept((Event<?>) signal);
            } else {
                LOGGER.debug(
                        "Dropping incoming event as no subscription for consuming events was registered. Did you call " +
                                "'client.startConsumingEvents()' ?");
            }
        } else {
            LOGGER.warn("Got unknown message on websocket: {}", message);
        }
    }

    private CompletableFuture<Void> addPendingAcknowledgement(final String protocolMessage) {
        return pendingAcknowledgements.compute(ack(protocolMessage), (k, v) -> {
            if (v != null) {
                throw new IllegalStateException(String.format("Second %s before first ack!", protocolMessage));
            } else {
                return new CompletableFuture<>();
            }
        });
    }

    private static String ack(final String protocolMessage) {
        return protocolMessage + ":ACK";
    }

    /**
     * Simple wrapper that catches exceptions of future.get() and wraps them in a {@link RuntimeException}.
     */
    private static <T> void safeGet(final ListenableFuture<T> future) throws RuntimeException {
        try {
            future.get(CONNECTION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            LOGGER.error("Interrupted by exception.", e);
            throw new RuntimeException("Error waiting for response to authentication request.", e);
        }
    }

    private static final class PendingResponse {

        private final CompletableFuture<CommandResponse<?>> responseFuture;

        private PendingResponse(final CompletableFuture<CommandResponse<?>> responseFuture) {
            this.responseFuture = responseFuture;
        }

        public static PendingResponse of(final CompletableFuture<CommandResponse<?>> responseFuture) {
            return new PendingResponse(responseFuture);
        }

        public CompletableFuture<CommandResponse<?>> getResponseFuture() {
            return responseFuture;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final PendingResponse that = (PendingResponse) o;
            return Objects.equals(responseFuture, that.responseFuture);
        }

        @Override
        public int hashCode() {
            return Objects.hash(responseFuture);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + " [" +
                    "responseFuture=" + responseFuture +
                    "]";
        }
    }

}
