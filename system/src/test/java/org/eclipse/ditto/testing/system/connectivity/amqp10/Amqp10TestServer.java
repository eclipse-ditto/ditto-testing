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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.DeliveryState;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonMessageHandler;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonServer;
import io.vertx.proton.ProtonServerOptions;
import io.vertx.proton.ProtonSession;

/**
 * Test AMQP1.0 server/endpoint in the same JVM. NOT intended for high throughput.
 */
final class Amqp10TestServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Amqp10TestServer.class);

    private final ConcurrentMap<String, ProtonSender> senders = new ConcurrentSkipListMap<>();
    private Handler<ProtonSender> senderOpenHandler = this::openSender;

    private final Set<ProtonConnection> openConnections = ConcurrentHashMap.newKeySet();
    private final int protonReceiverPrefetch;

    private final int port;
    private final MockSaslAuthenticatorFactory mockSaslAuthenticatorFactory = new MockSaslAuthenticatorFactory();

    private Vertx vertx;
    private ProtonServer server;

    // hub simulation
    private int sourceNotFoundAttempts = 0;
    private boolean initialConnection = false;

    Amqp10TestServer(final int port) {
        this(port, 1000);
    }

    /**
     * See {@link io.vertx.proton.ProtonReceiver#setPrefetch(int)}
     */
    Amqp10TestServer(final int port, final int protonReceiverPrefetch) {
        this.port = port;
        this.protonReceiverPrefetch = protonReceiverPrefetch;
    }

    /**
     * Run Amqp10TestServer locally at port 5673.
     *
     * @param args command-line arguments; ignored.
     * @throws Exception if the test server cannot start at port 5673.
     */
    public static void main(String... args) throws Exception {
        final Amqp10TestServer server = new Amqp10TestServer(5673);
        server.startServer();
        server.startStdInListener();
    }

    CompletableFuture<Void> stopProtonServer() {
        LOGGER.info("Proton Server is shutting down...");
        final CompletableFuture<Void> future = new CompletableFuture<>();

        server.close((res) -> {
            if (res.succeeded()) {
                future.complete(null);
            } else {
                LOGGER.error("Could not listen on AMQP 1.0 server port", res.cause());
                future.completeExceptionally(res.cause());
            }
        });

        return future;
    }

    void enableHonoBehaviour(final String connectionName) {
        this.senderOpenHandler = sender -> {
            // Simulate the behaviour observed with Eclipse Hono that a source is not available for a short time.
            // This resulted in the connection being established with the JMS Client, but opening the link failed
            // without an exception/notification and thus not triggering a reconnect.
            final String sourceAddress = sender.getRemoteSource().getAddress();

            if (sourceAddress.startsWith(connectionName)) {
                if (!initialConnection) { // if there was no initial connection allow
                    this.openSender(sender);
                    initialConnection = true;
                    LOGGER.info("Initial connection to HUB established.");
                } else if (sourceAddress.startsWith(connectionName)) {
                    // after the client reconnected answer the first two link establishment requests with "NOT_FOUND"
                    if (++sourceNotFoundAttempts > 2) {
                        LOGGER.error("The source with address <{}> is now available. Opening Link...", sourceAddress);
                        openSender(sender);
                        sourceNotFoundAttempts = 0;
                    } else {
                        LOGGER.error("The source with address <{}> was not found. Closing link. Attempt {}.",
                                sourceAddress,
                                sourceNotFoundAttempts);
                        sender.setCondition(new ErrorCondition(AmqpError.NOT_FOUND, "Node not found")).close();
                    }
                }
            } else {
                // normal behaviour for other connections
                this.openSender(sender);
            }
        };
    }

    boolean hasOpenConnection(final String remoteContainer) {
        return openConnections.stream()
                .anyMatch(conn -> remoteContainer.equals(conn.getRemoteContainer()));
    }

    /**
     * Starts the AMQP1.0 server.
     */
    void startServer() throws Exception {
        LOGGER.info("Starting AMQP 1.0 server");

        // Create the Vert.x instance
        vertx = Vertx.vertx();
        vertx.exceptionHandler(t -> LOGGER.error("Exception caught in global error handler: {}", t.getMessage(), t));
        final ProtonServerOptions options = new ProtonServerOptions();
        options.setLogActivity(true);

        // Create the Vert.x AMQP client instance
        server = createProtonServer(vertx, options, 7, 1000L);
        LOGGER.info("AMQP1.0 server is started.");
    }

    private void startStdInListener() {
        final Scanner scanner = new Scanner(System.in);
        String line;
        while ((line = scanner.nextLine()) != null) {
            if (line.isEmpty() || !line.contains("#")) {
                System.err.println("Use the format address#message in order to invoke sending a message");
                System.err.println(
                        "Or use the format address#message#headerA=42;headerB=yes in order to invoke sending a message with specified headers");
            } else {
                final String[] split = line.split("#", 3);
                final String address = split[0];
                final String textMsg = split[1];
                final Map<String, Object> headers;
                if (split.length == 3) {
                    headers = Arrays.stream(split[2].split(";"))
                            .collect(Collectors.toMap(
                                    h -> h.substring(0, h.indexOf('=')),
                                    h -> h.substring(h.indexOf('=') + 1))
                            );
                } else {
                    headers = Collections.emptyMap();
                }
                final Message message = Message.Factory.create();
                message.setAddress(address);
                message.setBody(new AmqpValue(textMsg));
                message.setCorrelationId(UUID.randomUUID().toString());
                Optional.ofNullable(headers.get("reply-to")).map(String::valueOf)
                        .ifPresent(message::setReplyTo);
                Optional.ofNullable(headers.get("content-type")).map(String::valueOf)
                        .ifPresent(message::setContentType);
                headers.remove("reply-to");
                headers.remove("content-type");
                message.setApplicationProperties(new ApplicationProperties(headers));
                sendMessageToClient(message);
            }
        }
    }

    private ProtonServer createProtonServer(final Vertx vertx,
            final ProtonServerOptions options,
            final int retries,
            final long backoffMillis) throws Exception {

        final CompletableFuture<AsyncResult<ProtonServer>> nextAttemptFuture = new CompletableFuture<>();
        ProtonServer.create(vertx, options)
                .saslAuthenticatorFactory(mockSaslAuthenticatorFactory)
                .connectHandler(this::processConnection)
                .listen(port, nextAttemptFuture::complete);
        final AsyncResult<ProtonServer> nextResult = nextAttemptFuture.get();
        if (nextResult.succeeded()) {
            final ProtonServer nextServer = nextResult.result();
            LOGGER.info("AMQP 1.0 server listening on port: {}", nextServer.actualPort());
            return nextServer;
        } else {
            LOGGER.error("Could not listen on AMQP 1.0 server port {}, remaining attempts: {}.",
                    port, retries, nextResult.cause());
            if (retries > 0) {
                Thread.sleep(backoffMillis);
                return createProtonServer(vertx, options, retries - 1, backoffMillis * 2);
            } else {
                throw new RuntimeException("Failed to start AMQP 1.0 server.");
            }
        }
    }

    /**
     * Stops the AMQP1.0 server and the vertx instance.
     */
    void stopServer() throws Exception {
        runAndWait("Stopping AMQP 1.0 server", handler -> vertx.runOnContext(v -> server.close(handler)));
        runAndWait("Stopping vertx instance", handler -> vertx.close(handler));
    }

    MockSaslAuthenticatorFactory getMockSaslAuthenticatorFactory() {
        return mockSaslAuthenticatorFactory;
    }

    /**
     * Sends a message from this AMQP 1.0 server to a randomly selected client.
     *
     * @param message the Message to send to a connected client
     */
    private void sendMessageToClient(final Message message) {
        final ProtonSender protonSender = this.senders.get(message.getAddress());

        if (protonSender != null) {
            LOGGER.info("Sending message with correlation-id <{}>: {}", message.getCorrelationId(), message);
            vertx.runOnContext(v -> sendMessageWithRedelivery(protonSender, message));
        } else {
            LOGGER.warn("No ProtonSender available for sending message to address {}. The following addresses are " +
                    "supported: {}.", message.getAddress(), senders.keySet());
        }
    }

    private void sendMessageWithRedelivery(final ProtonSender protonSender, final Message message) {
        protonSender.send(message, delivery -> {
            final DeliveryState remoteState = delivery.getRemoteState();
            LOGGER.info("The message with correlation-id <{}> was received by the client in state <{}>. " +
                            "local state: <{}>, message id: <{}>, address: <{}>",
                    message.getCorrelationId(), remoteState, delivery.getLocalState(), message.getMessageId(),
                    message.getAddress());
            delivery.disposition(delivery.getRemoteState(), true);
            // redelivery
            final boolean isTarget = message.getAddress() != null && message.getAddress().contains("Target");
            if (!isTarget && remoteState.getType() == DeliveryState.DeliveryStateType.Modified) {
                final Modified modified = (Modified) remoteState;
                if (modified.getDeliveryFailed() == Boolean.TRUE && modified.getUndeliverableHere() != Boolean.TRUE) {
                    LOGGER.info("Retransmitting message due to failed remote delivery state");
                    sendMessageWithRedelivery(protonSender, message);
                }
            } else if (isTarget) {
                LOGGER.info("Skipping redelivery to target address.");
            }
        });

    }

    private void processConnection(final ProtonConnection connection) {
        connection.openHandler(log(res -> {
            LOGGER.info("Client connected: {} [{}]", connection.getRemoteContainer(), connection.getRemoteHostname());
            connection.setContainer(connection.getRemoteContainer());
            connection.open();
            openConnections.add(connection);
        })).closeHandler(log(c -> {
            LOGGER.info("Client closing AMQP connection: {}", connection.getRemoteContainer());
            connection.close();
            openConnections.remove(connection);
            LOGGER.info("Client closed AMQP connection: {}", connection.getRemoteContainer());
        })).disconnectHandler(log(c -> {
            LOGGER.info("Client socket disconnected: {}", connection.getRemoteContainer());
            connection.disconnect();
            openConnections.remove(connection);
            LOGGER.info("Client socket disconnected handling finished: {}", connection.getRemoteContainer());
        })).sessionOpenHandler(log(protonSession -> {
            LOGGER.debug("Session open");
            protonSession.closeHandler(log(closeHandler -> {
                final ProtonSession session = closeHandler.result();
                LOGGER.info("Session closed remotely.");
                session.close();
                session.free();
            })).open();
        })).receiverOpenHandler(log(receiver -> {
            final String targetAddress = receiver.getRemoteTarget().getAddress();
            LOGGER.info("Client [{} via {}]  requests to open SENDER at {}.", connection.getContainer(),
                    connection.getRemoteHostname(), targetAddress);

            receiver.setPrefetch(this.protonReceiverPrefetch);
            receiver.setTarget(receiver.getRemoteTarget()) // This is rather naive, for example use only
                    .handler(log((delivery, msg) -> {

                        LOGGER.info("[{}] Received delivery at {} from {}: cor-id: {} - properties: {} - " +
                                        "applicationProperties: {}",
                                connection.getContainer(),
                                msg.getAddress(), connection.getRemoteHostname(), msg.getCorrelationId(),
                                msg.getProperties(), msg.getApplicationProperties());
                        delivery.disposition(new Accepted(), true);
                        final String address = msg.getAddress();
                        if (address != null) {
                            final Section body = msg.getBody();
                            if (body instanceof AmqpValue) {
                                final String content = (String) ((AmqpValue) body).getValue();
                                LOGGER.debug("Message to: {}, cid: {}, body: {}", address, msg.getCorrelationId(),
                                        content);
                            }
                            sendMessageToClient(msg);
                        } else {
                            LOGGER.warn("No address in message");
                        }
                    })).closeHandler(log(closeHandler -> {
                final ProtonReceiver protonReceiver = closeHandler.result();
                LOGGER.info("Receiver {} closed remotely.", protonReceiver.getName());
                protonReceiver.close();
                protonReceiver.free();
            })).open();
            LOGGER.info("[{}] Opening inbound link at {}", connection.getContainer(), targetAddress);
        })).senderOpenHandler(log(this.senderOpenHandler));
    }

    private void openSender(final ProtonSender sender) {
        final String sourceAddress = sender.getRemoteSource().getAddress();
        LOGGER.info("[{}] Opening outbound link at '{}'", sender.getSession().getConnection().getContainer(),
                sourceAddress);
        senders.put(sourceAddress, sender);
        sender.setSource(sender.getRemoteSource()); // This is rather naive, for example use only
        sender.closeHandler(closeHandler -> {
            final ProtonSender protonSender = closeHandler.result();
            LOGGER.info("Sender {} closed remotely.", protonSender.getName());
            protonSender.close();
            protonSender.free();
        });
        sender.open();
        LOGGER.info("Adding to the list of available senders for address: '{}'", sourceAddress);
    }

    /**
     * Wraps and logs any Throwable thrown in the given handler that might be lost otherwise.
     *
     * @param protonMessageHandler the handler to wrap
     * @return the wrapping handler
     */
    private static ProtonMessageHandler log(final ProtonMessageHandler protonMessageHandler) {
        return (delivery, message) -> {
            try {
                protonMessageHandler.handle(delivery, message);
            } catch (Throwable t) {
                LOGGER.error("Exception occurred in handler: {}", t.getMessage(), t);
                rethrow(t);
            }
        };
    }

    /**
     * Wraps and logs any Throwable thrown in the given handler that might be lost otherwise.
     *
     * @param handler the handler to wrap
     * @return the wrapping handler
     */
    private static <E> Handler<E> log(final Handler<E> handler) {
        return event -> {
            try {
                handler.handle(event);
            } catch (Throwable t) {
                LOGGER.error("Exception occurred in handler: {}", t.getMessage(), t);
                rethrow(t);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void rethrow(Throwable e) throws T {
        throw (T) e;
    }

    private static void runAndWait(final String description, final Consumer<Handler<AsyncResult<Void>>> closeRunnable)
            throws Exception {
        final CompletableFuture<Void> stopFuture = new CompletableFuture<>();
        final Handler<AsyncResult<Void>> resultHandler = stopHandler -> {
            if (stopHandler.succeeded()) {
                stopFuture.complete(null);
                LOGGER.info("Done: {}", description);
            } else {
                LOGGER.error("Failed: {}", description, stopHandler.cause());
                stopFuture.completeExceptionally(stopHandler.cause());
            }
        };
        LOGGER.info(description);
        closeRunnable.accept(resultHandler);
        stopFuture.get(5, TimeUnit.SECONDS);
    }

}
