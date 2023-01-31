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
package org.eclipse.ditto.testing.common.ws;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.proxy.ProxyServer;
import org.asynchttpclient.ws.WebSocket;
import org.asynchttpclient.ws.WebSocketListener;
import org.asynchttpclient.ws.WebSocketUpgradeHandler;
import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.base.model.exceptions.DittoRuntimeException;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.headers.WithDittoHeaders;
import org.eclipse.ditto.base.model.headers.translator.HeaderTranslator;
import org.eclipse.ditto.base.model.json.Jsonifiable;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.announcements.Announcement;
import org.eclipse.ditto.base.model.signals.commands.Command;
import org.eclipse.ditto.base.model.signals.commands.CommandResponse;
import org.eclipse.ditto.base.model.signals.events.Event;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
import org.eclipse.ditto.protocol.adapter.ProtocolAdapter;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.client.ditto_protocol.DittoProtocolClient;
import org.eclipse.ditto.testing.common.client.ditto_protocol.HeaderBlocklistChecker;
import org.eclipse.ditto.testing.common.client.ditto_protocol.options.Option;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.util.StringUtil;
import org.eclipse.ditto.things.model.signals.commands.ThingErrorResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.util.Try;

/**
 * Client for Things-WS.
 */
public final class ThingsWebsocketClient implements WebSocketListener, AutoCloseable, DittoProtocolClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThingsWebsocketClient.class);

    private static final String ACK = ":ACK";
    private static final String PROTOCOL_CMD_START_SEND_EVENTS = "START-SEND-EVENTS";
    private static final String PROTOCOL_CMD_START_SEND_EVENTS_ACK = PROTOCOL_CMD_START_SEND_EVENTS.concat(ACK);
    private static final String PROTOCOL_CMD_STOP_SEND_EVENTS = "STOP-SEND-EVENTS";
    private static final String PROTOCOL_CMD_STOP_SEND_EVENTS_ACK = PROTOCOL_CMD_STOP_SEND_EVENTS.concat(ACK);
    private static final String PROTOCOL_CMD_JWT_TOKEN_TEMPLATE = "JWT-TOKEN?jwtToken=%s";

    private static final long CONNECTION_TIMEOUT_MILLIS = 5000;
    private static final long ROUNDTRIP_TIMEOUT_SECONDS = 20L;

    private final DefaultAsyncHttpClient client;
    private final ProtocolAdapter protocolAdapter;
    private final String endpoint;
    private final String jwt;

    private final AtomicReference<WebSocket> webSocketReference = new AtomicReference<>();
    private final AtomicReference<Consumer<Adaptable>> adaptableConsumerReference = new AtomicReference<>();
    private final ConcurrentHashMap<String, PendingResponse> pendingResponses;
    private final ConcurrentHashMap<String, CompletableFuture<Void>> waitForAckStages;
    private final Map<String, String> additionalHttpHeaders;
    private final JwtAuthMethod authMethod;

    private final List<Throwable> errors;

    private ThingsWebsocketClient(final String endpoint,
            final String jwt,
            final Map<String, String> additionalHttpHeaders,
            @Nullable final ProxyServer proxyServer,
            final JwtAuthMethod authMethod) {

        final var configBuilder = new DefaultAsyncHttpClientConfig.Builder();
        configBuilder.setThreadPoolName("things-ws");
        configBuilder.setUserAgent("ThingsWebSocketClient");
        configBuilder.setWebSocketMaxFrameSize(256 * 1024); // 1 Ditto protocol message can reach 250 KB
        if (proxyServer != null) {
            configBuilder.setProxyServer(proxyServer);
        }
        // make IO single-threaded because this client is used for command order test
        configBuilder.setIoThreadsCount(1);

        client = new DefaultAsyncHttpClient(configBuilder.build());
        protocolAdapter = DittoProtocolAdapter.of(HeaderTranslator.empty());
        this.endpoint = endpoint;
        this.jwt = jwt;
        pendingResponses = new ConcurrentHashMap<>();
        waitForAckStages = new ConcurrentHashMap<>();
        this.additionalHttpHeaders = Map.copyOf(additionalHttpHeaders);
        this.authMethod = authMethod;
        this.errors = new ArrayList<>();
    }

    public static ThingsWebsocketClient newInstance(final String endpoint,
            final String jwt,
            final Map<String, String> additionalHttpHeaders,
            @Nullable final ProxyServer proxyServer,
            final JwtAuthMethod authMethod) {

        return new ThingsWebsocketClient(ConditionChecker.checkNotNull(endpoint, "endpoint"),
                ConditionChecker.checkNotNull(jwt, "jwt"),
                ConditionChecker.checkNotNull(additionalHttpHeaders, "additionalHttpHeaders"),
                proxyServer,
                ConditionChecker.checkNotNull(authMethod, "authMethod"));
    }

    public List<Throwable> getErrors() {
        return errors;
    }

    /**
     * Calls {@link ThingsWebsocketClient#connect(CharSequence)} with a random
     * correlation ID.
     * For WebSockets you should use the other connect method, the one with correlation ID.
     */
    @Override
    public void connect() {
        connect(CorrelationId.random());
    }

    /**
     * Connects the client.
     */
    public void connect(final CharSequence correlationId) {
        LOGGER.info("Connecting WebSocket client to endpoint <{}> ...", endpoint);
        final var upgradeHandler = new WebSocketUpgradeHandler.Builder()
                .addWebSocketListener(this)
                .build();
        final String endpointWithParameter;
        if (authMethod == JwtAuthMethod.QUERY_PARAM) {
            endpointWithParameter = String.format("%s?%s=%s", endpoint, "access_token", jwt);
        } else {
            endpointWithParameter = endpoint;
        }

        final var requestBuilder = client.prepareGet(endpointWithParameter)
                .addHeader(HttpHeader.X_CORRELATION_ID.getName(), correlationId);

        if (authMethod == JwtAuthMethod.HEADER) {
            requestBuilder.addHeader(HttpHeader.AUTHORIZATION.getName(), "Bearer " + jwt);
        }

        additionalHttpHeaders.forEach(requestBuilder::addHeader);
        final var execute = requestBuilder.execute(upgradeHandler);

        safeGet(execute);
    }

    /**
     * Disconnects to client.
     */
    @Override
    public void disconnect() {
        webSocketReference.set(null);
        try {
            LOGGER.info("Disconnecting WebSocket client from <{}> ...", endpoint);
            client.close();
            LOGGER.info("Disconnected WebSocket client from <{}>.", endpoint);
        } catch (final Exception e) {
            LOGGER.info("Exception occurred while disconnecting WebSocket client from endpoint <{}>.", endpoint, e);
        }
    }

    /**
     * Refreshes authentication.
     *
     * @param jwt a valid token.
     */
    public void refresh(final String jwt) {
        final var command = String.format(PROTOCOL_CMD_JWT_TOKEN_TEMPLATE, jwt);
        sendWithResponse(command, null).join();
    }

    /**
     * Emits in fire-and-forget fashion a {@link Signal}.
     *
     * @param signal the signal to send.
     */
    public void emit(final Signal<?> signal) {
        emitWithoutResponse(ProtocolFactory.wrapAsJsonifiableAdaptable(protocolAdapter.toAdaptable(signal)));
    }

    public void emitWithoutResponse(final Jsonifiable<?> jsonifiable) {
        final var webSocket = webSocketReference.get();
        final var text = jsonifiable.toJsonString();
        if (webSocket != null) {
            LOGGER.info("Emitting text frame <{}>.", text);
            webSocket.sendTextFrame(text);
        }
    }

    /**
     * Sends a {@link Command}.
     *
     * @param command the command to send.
     * @return the response to the command.
     */
    @Override
    public CompletableFuture<CommandResponse<?>> send(final Command<?> command, final Option<?>... options) {
        return sendAdaptable(protocolAdapter.toAdaptable(command));
    }

    public void sendWithoutResponse(final String command) {
        sendWithResponse(command, null).join();
    }

    public CompletableFuture<CommandResponse<?>> sendWithResponse(final String command,
            @Nullable final String correlationId) {

        final var completableFuture = new CompletableFuture<CommandResponse<?>>();

        final var webSocket = webSocketReference.get();
        if (webSocket != null) {
            if (correlationId != null) {
                pendingResponses.compute(correlationId, (key, previousPendingResponse) -> {
                    if (previousPendingResponse != null) {
                        previousPendingResponse.getResponseFuture()
                                .completeExceptionally(new IllegalStateException("Duplicate correlation ID: " + key));
                    }
                    return PendingResponse.of(completableFuture);
                });
            } else {
                completableFuture.complete(null);
            }
            LOGGER.info("Sending JSON: {}", command);
            webSocket.sendTextFrame(command);
        } else {
            completableFuture.completeExceptionally(new RuntimeException("WebSocket is not connected!"));
        }

        new CompletableFuture<Void>().completeOnTimeout(null, ROUNDTRIP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .thenApply(unused ->
                        completableFuture.completeExceptionally(new TimeoutException("Did not receive a response")));

        return completableFuture;
    }

    /**
     * Request backend to send twin events and handle them.
     *
     * @param eventConsumer a consumer for all incoming twin events.
     */
    public CompletableFuture<Void> startConsumingEvents(final Consumer<Event<?>> eventConsumer) {
        adaptableConsumerReference.set(fromEventConsumer(eventConsumer));
        return sendProtocolCommand(PROTOCOL_CMD_START_SEND_EVENTS, PROTOCOL_CMD_START_SEND_EVENTS_ACK);
    }

    private Consumer<Adaptable> fromEventConsumer(final Consumer<Event<?>> eventConsumer) {
        return adaptable -> {
            final Jsonifiable<JsonObject> jsonifiable = protocolAdapter.fromAdaptable(adaptable);
            if (jsonifiable instanceof Event) {
                LOGGER.debug("Received Event JSON <{}>.", jsonifiable);
                eventConsumer.accept((Event<?>) jsonifiable);
            }
        };
    }

    /**
     * Request backend to send twin events filtered by the given rql and handle them.
     *
     * @param eventConsumer a consumer for all incoming twin events.
     */
    public CompletableFuture<Void> startConsumingEvents(final Consumer<Event<?>> eventConsumer,
            final String rqlFilter) {

        adaptableConsumerReference.set(fromEventConsumer(eventConsumer));
        return sendProtocolCommand(PROTOCOL_CMD_START_SEND_EVENTS + "?filter=" + rqlFilter,
                PROTOCOL_CMD_START_SEND_EVENTS_ACK);
    }

    /**
     * Send a protocol command.
     *
     * @param protocolCommand the protocol command to send.
     * @param acknowledgement the expected acknowledgement.
     * @return a future that completes when the protocol command is acknowledged.
     */
    public CompletableFuture<Void> sendProtocolCommand(final String protocolCommand, final String acknowledgement) {
        return waitForAckStages.compute(acknowledgement, (k, previousFuture) -> {
            final CompletableFuture<Void> future;
            if (previousFuture == null) {
                future = new CompletableFuture<>();
                new CompletableFuture<Void>().completeOnTimeout(null, ROUNDTRIP_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .thenAccept(unused -> {
                            if (!future.isDone()) {
                                future.completeExceptionally(
                                        new TimeoutException("Did not receive " + acknowledgement));
                            }
                        });
            } else {
                future = previousFuture;
                LOGGER.warn("Sending <{}>, but future for <{}> exists.", protocolCommand, acknowledgement);
            }
            webSocketReference.get().sendTextFrame(protocolCommand);
            return future;
        });
    }

    /**
     * Set the adaptable consumer.
     *
     * @param adaptableConsumer consumer for all incoming adaptable.
     */
    public void onAdaptable(final Consumer<Adaptable> adaptableConsumer) {
        this.adaptableConsumerReference.set(adaptableConsumer);
    }

    /**
     * Replace the adaptable consumer by a consumer from signals from the incoming adaptables.
     *
     * @param signalConsumer the signal consumer.
     */
    public void onSignal(final Consumer<Signal<?>> signalConsumer) {
        adaptableConsumerReference.set(adaptable -> signalConsumer.accept(protocolAdapter.fromAdaptable(adaptable)));
    }

    /**
     * Stops consuming {@link Event}s.
     */
    public CompletableFuture<Void> stopConsumingEvents() {
        adaptableConsumerReference.set(null);
        return sendProtocolCommand(PROTOCOL_CMD_STOP_SEND_EVENTS, PROTOCOL_CMD_STOP_SEND_EVENTS_ACK);
    }

    public boolean isOpen() {
        final var webSocket = webSocketReference.get();
        return null != webSocket && webSocket.isOpen();
    }

    @Override
    public void onBinaryFrame(final byte[] message, final boolean finalFragment, final int rsv) {
        final var stringMessage = new String(message, StandardCharsets.UTF_8);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Received WebSocket byte array message '{}', as string '{}' - don't know what to do with it!.",
                    message,
                    StringUtil.truncate(stringMessage));
        }
    }

    @Override
    public void onTextFrame(final String message, final boolean finalFragment, final int rsv) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Received WebSocket string message '{}'.", message);
        }
        if (isAcknowledgement(message)) {
            handleAcknowledgementMessage(message);
        } else {
            final var messageJsonTry = tryToParseJsonObjectFromMessage(message);
            if (messageJsonTry.isSuccess()) {
                handleJsonMessage(messageJsonTry.get());
            } else {
                final var failure = messageJsonTry.failed();
                LOGGER.warn("Got unknown non-JSON message on WebSocket:\n{}", message, failure.get());
            }
        }
    }

    private static boolean isAcknowledgement(final String message) {
        return message.endsWith(ACK);
    }

    private void handleAcknowledgementMessage(final String acknowledgementMessage) {
        waitForAckStages.computeIfPresent(acknowledgementMessage, (k, future) -> {
            future.complete(null);
            return null;
        });
    }

    private static Try<JsonObject> tryToParseJsonObjectFromMessage(final String message) {
        return Try.apply(() -> JsonObject.of(message));
    }

    private void handleJsonMessage(final JsonObject message) {
        validateHeaders(message);

        final var jsonifiableAdaptable = ProtocolFactory.jsonifiableAdaptableFromJson(message);

        final var isConsumed = consumeJsonifiableAdaptable(jsonifiableAdaptable);

        final var pendingResponse = getPendingResponse(jsonifiableAdaptable);
        if (pendingResponse.isPresent()) {
            completePendingResponse(pendingResponse.get(), protocolAdapter.fromAdaptable(jsonifiableAdaptable));
        } else if (!isConsumed) {
            final Jsonifiable<JsonObject> jsonifiable = protocolAdapter.fromAdaptable(jsonifiableAdaptable);
            if (!(jsonifiable instanceof Event) && !(jsonifiable instanceof Announcement)) {

                // Events are already handled by adaptableConsumer.
                LOGGER.warn("Got unknown message on WebSocket: {}", message);
            }
        }
    }

    private static void validateHeaders(final JsonObject message) {
        message.getValue(JsonifiableAdaptable.JsonFields.HEADERS)
                .ifPresent(HeaderBlocklistChecker::assertHeadersDoNotIncludeBlocklisted);
    }

    private boolean consumeJsonifiableAdaptable(final Adaptable jsonifiableAdaptable) {
        final boolean consumed;
        final var adaptableConsumer = adaptableConsumerReference.get();
        if (adaptableConsumer != null) {
            adaptableConsumer.accept(jsonifiableAdaptable);
            consumed = true;
        } else {
            consumed = false;
        }
        return consumed;
    }

    private Optional<PendingResponse> getPendingResponse(final WithDittoHeaders jsonifiableAdaptable) {
        final var dittoHeaders = jsonifiableAdaptable.getDittoHeaders();
        return dittoHeaders.getCorrelationId()
                .map(pendingResponses::remove);
    }

    private static void completePendingResponse(final PendingResponse pendingResponse,
            final Jsonifiable<JsonObject> jsonifiable) {

        final var responseFuture = pendingResponse.getResponseFuture();
        if (jsonifiable instanceof CommandResponse) {
            responseFuture.complete((CommandResponse<?>) jsonifiable);
        } else if (jsonifiable instanceof DittoRuntimeException) {
            final var errorResponse = ThingErrorResponse.of((DittoRuntimeException) jsonifiable);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Received Error JSON: {}", jsonifiable.toJsonString());
            }
            responseFuture.complete(errorResponse);
        }
    }

    @Override
    public void onOpen(final WebSocket webSocket) {
        LOGGER.info("Connected WebSocket client to <{}>.", endpoint);
        webSocketReference.set(webSocket);
    }

    @Override
    public void onClose(final WebSocket webSocket, final int code, final String reason) {
        LOGGER.info("Connection to <{}> closed by remote (code: {}, reason: {}).", endpoint, code, reason);
    }

    @Override
    public void onError(final Throwable throwable) {
        errors.add(throwable);
        if (throwable instanceof DittoRuntimeException) {
            LOGGER.warn("Got DittoRuntimeException which should not happen during runtime: {}",
                    throwable.getMessage(),
                    throwable);
        } else {
            if (null == throwable) {
                LOGGER.error("Unknown error in WebSocket occurred.");
            } else {
                LOGGER.error("Error in WebSocket occurred: {}", throwable.getMessage(), throwable);
            }
        }
    }

    /**
     * Send an adaptable as text frame.
     *
     * @param adaptable the adaptable to send.
     * @return a future that completes with any response if the adaptable is a command with correlation ID.
     */
    @Override
    public CompletableFuture<CommandResponse<?>> sendAdaptable(final Adaptable adaptable, Option<?>... options) {
        final var correlationId = getCorrelationIdIfResponseRequired(adaptable.getDittoHeaders());

        final var stringMessage = ProtocolFactory.wrapAsJsonifiableAdaptable(adaptable).toJsonString();
        return sendWithResponse(stringMessage, correlationId);
    }

    @Nullable
    private static String getCorrelationIdIfResponseRequired(final DittoHeaders headers) {
        if (headers.isResponseRequired()) {
            return headers.getCorrelationId().orElse(null);
        }
        return null;
    }

    /**
     * Simple wrapper that catches exception of future.get() and wraps them in a {@link RuntimeException}.
     */
    private static <T> void safeGet(final ListenableFuture<T> future) throws CompletionException {
        try {
            future.get(CONNECTION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            LOGGER.error("Failure while waiting for future", e);
            throw new CompletionException(e);
        }
    }

    @Override
    public void close() {
        disconnect();
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
            final var that = (PendingResponse) o;
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

    public enum JwtAuthMethod {

        /**
         * The JWT is passed as header 'Authorization Bearer [jwt]'
         */
        HEADER,

        /**
         * The JWT is passed as query parameter 'ws/2?access_token=[jwt]'
         */
        QUERY_PARAM
    }

}
