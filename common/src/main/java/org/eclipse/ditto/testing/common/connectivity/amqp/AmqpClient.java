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
package org.eclipse.ditto.testing.common.connectivity.amqp;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.net.URI;
import java.text.MessageFormat;
import java.util.Hashtable;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import javax.annotation.Nullable;
import javax.jms.CompletionListener;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.JmsTextMessage;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacade;
import org.apache.qpid.proton.amqp.Symbol;
import org.eclipse.ditto.base.model.headers.WithDittoHeaders;
import org.eclipse.ditto.base.model.headers.translator.HeaderTranslator;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.commands.Command;
import org.eclipse.ditto.base.model.signals.commands.CommandResponse;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonParseException;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
import org.eclipse.ditto.protocol.adapter.ProtocolAdapter;
import org.eclipse.ditto.testing.common.client.ditto_protocol.DittoProtocolClient;
import org.eclipse.ditto.testing.common.client.ditto_protocol.HeaderBlocklistChecker;
import org.eclipse.ditto.testing.common.client.ditto_protocol.options.Option;
import org.eclipse.ditto.testing.common.client.ditto_protocol.options.OptionsMap;
import org.eclipse.ditto.testing.common.connectivity.amqp.options.AmqpClientOptionDefinitions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A very basic AMQP client implementation to send Ditto Protocol messages to a connection source.
 * <p>
 * possible improvements: message handling could be implemented similar to the ws client
 * {@link org.eclipse.ditto.testing.common.ws.ThingsWebsocketClient}, to be more thread safe.
 */
public final class AmqpClient implements DittoProtocolClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpClient.class);

    private static final long ROUND_TRIP_TIMEOUT_MS = 10_000L;

    private final URI endpointUri;
    private final String connectionName;
    private final String sourceAddress;
    private final String targetAddress;
    private final String replyAddress;
    private final ProtocolAdapter protocolAdapter;
    private final AtomicReference<Consumer<TextMessage>> textMessageConsumerReference;
    private final AtomicReference<Consumer<Signal<?>>> signalConsumerReference;

    private Connection connection;
    private Session sessionDeviceToCloud;
    private Session sessionCloudToDevice;
    private Queue targetQueue;
    private MessageConsumer targetConsumer;

    private AmqpClient(final URI endpointUri,
            final String connectionName,
            final String sourceAddress,
            final String targetAddress,
            final String replyAddress) {

        this.endpointUri = checkNotNull(endpointUri, "endpointUri");
        this.connectionName = checkNotNull(connectionName, "connectionName");
        this.sourceAddress = checkNotNull(sourceAddress, "sourceAddress");
        this.targetAddress = checkNotNull(targetAddress, "targetAddress");
        this.replyAddress = checkNotNull(replyAddress, "replyAddress");
        protocolAdapter = DittoProtocolAdapter.of(HeaderTranslator.empty());
        textMessageConsumerReference = new AtomicReference<>();
        signalConsumerReference = new AtomicReference<>();

        connection = null;
        sessionDeviceToCloud = null;
        sessionCloudToDevice = null;
        targetQueue = null;
        targetConsumer = null;
    }

    public static AmqpClient newInstance(final URI endpointUri,
            final String connectionName,
            final String sourceAddress,
            final String targetAddress,
            final String replyAddress) {

        return new AmqpClient(endpointUri, connectionName, sourceAddress, targetAddress, replyAddress);
    }

    @Override
    public void connect() {
        connection = createConnectionOrThrow();
        startConnectionOrThrow(connection);
        sessionDeviceToCloud = createSessionOrThrow(connection);
        sessionCloudToDevice = createSessionOrThrow(connection);
        initMessageConsumers();
    }

    private Connection createConnectionOrThrow() {
        try {
            return createConnection();
        } catch (final NamingException | JMSException e) {
            final var pattern = "Failed to initialise connection: {0}";
            throw new IllegalStateException(MessageFormat.format(pattern, e.getMessage()), e);
        }
    }

    @SuppressWarnings("java:S1149")
    private Connection createConnection() throws NamingException, JMSException {
        final var initialContext = getInitialContext();
        final var factory = (ConnectionFactory) initialContext.lookup(connectionName);
        return factory.createConnection();
    }

    private InitialContext getInitialContext() throws NamingException {
        final var environment = new Hashtable<String, String>();
        environment.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        environment.put("connectionfactory." + connectionName, endpointUri.toString());
        return new InitialContext(environment);
    }

    private static void startConnectionOrThrow(final Connection connection) {
        try {
            connection.start();
        } catch (final JMSException e) {
            throw new IllegalStateException(MessageFormat.format("Failed to start connection: {0}", e.getMessage()), e);
        }
    }

    private static Session createSessionOrThrow(final Connection connection) {
        try {
            return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (final JMSException e) {
            throw new IllegalStateException(MessageFormat.format("Failed to create session: {0}", e.getMessage()), e);
        }
    }

    private void initMessageConsumers() {
        targetQueue = new JmsQueue(targetAddress);
        targetConsumer = tryToCreateMessageConsumer();
        tryToSetMessageListener(targetConsumer);
    }

    private MessageConsumer tryToCreateMessageConsumer() {
        try {
            return sessionCloudToDevice.createConsumer(targetQueue);
        } catch (final JMSException e) {
            throw new IllegalStateException(
                    MessageFormat.format("Failed to create message consumer: {0}", e.getMessage()),
                    e
            );
        }
    }

    private void tryToSetMessageListener(final MessageConsumer messageConsumer) {
        try {
            setMessageListener(messageConsumer);
        } catch (final JMSException e) {
            throw new IllegalStateException(
                    MessageFormat.format("Failed to set message listener: {0}", e.getMessage()),
                    e
            );
        }
    }

    private void setMessageListener(final MessageConsumer messageConsumer) throws JMSException {
        messageConsumer.setMessageListener(message -> {
            final var textMessageConsumer = textMessageConsumerReference.get();
            final var signalConsumer = signalConsumerReference.get();
            if (null != textMessageConsumer || null != signalConsumer) {
                getAsTextMessage(message)
                        .ifPresent(textMessage -> tryToHandleTextMessage(textMessage,
                                textMessageConsumer,
                                signalConsumer));
            } else {
                LOGGER.info("No message consumers registered.");
            }
        });
    }

    private static Optional<TextMessage> getAsTextMessage(final Message message) {
        final Optional<TextMessage> result;
        if (message instanceof TextMessage textMessage) {
            result = Optional.of(textMessage);
        } else {
            result = Optional.empty();
            LOGGER.info("Received unknown message type. Only text messages are supported.");
        }
        return result;
    }

    private void tryToHandleTextMessage(
            final TextMessage textMessage,
            @Nullable final Consumer<TextMessage> textMessageConsumer,
            @Nullable final Consumer<Signal<?>> signalConsumer
    ) {
        try {
            final var payload = textMessage.getText();
            LOGGER.info("Received text message with correlation-id <{}> and content: {}",
                    textMessage.getJMSCorrelationID(),
                    payload);
            if (null != textMessageConsumer) {
                textMessageConsumer.accept(textMessage);
            }
            if (null != signalConsumer) {
                signalConsumer.accept(getAsSignal(payload));
            }
        } catch (final JMSException e) {
            throw new IllegalStateException(
                    MessageFormat.format("Failed to handle TextMessage: {0}", e.getMessage()),
                    e
            );
        }
    }

    private Signal<?> getAsSignal(final String textMessagePayload) {
        return protocolAdapter.fromAdaptable(
                ProtocolFactory.jsonifiableAdaptableFromJson(
                        JsonObject.of(textMessagePayload)
                )
        );
    }

    @Override
    public CompletableFuture<CommandResponse<?>> sendAdaptable(final Adaptable adaptable, final Option<?>... options) {
        return CompletableFuture.supplyAsync(() -> {
            final var optionsMap = OptionsMap.of(options);
            final var message = tryToGetJmsTextMessage(adaptable, optionsMap);

            try (
                    final var messageProducer = sessionDeviceToCloud.createProducer(new JmsQueue(sourceAddress));
                    final var messageConsumer = sessionDeviceToCloud.createConsumer(message.getJMSReplyTo())
            ) {
                final var stringMessage = ProtocolFactory.wrapAsJsonifiableAdaptable(adaptable).toJsonString();

                LOGGER.info("Sending message <{}> to <{}>.", stringMessage, messageProducer.getDestination());
                messageProducer.send(message,
                        DeliveryMode.NON_PERSISTENT,
                        Message.DEFAULT_PRIORITY,
                        Message.DEFAULT_TIME_TO_LIVE,
                        new CompletionListener() {
                            @Override
                            public void onCompletion(final Message message) {
                                optionsMap.getValue(AmqpClientOptionDefinitions.Send.TEXT_MESSAGE_CONSUMER)
                                        .ifPresent(jmsTextMessageConsumer -> jmsTextMessageConsumer.accept((TextMessage) message));
                            }

                            @Override
                            public void onException(final Message message, final Exception exception) {
                                // Do nothing.
                            }
                        }
                );

                final var receivedMessage = (TextMessage) messageConsumer.receive(ROUND_TRIP_TIMEOUT_MS);
                optionsMap.getValue(AmqpClientOptionDefinitions.Receive.TEXT_MESSAGE_CONSUMER)
                        .ifPresent(jmsTextMessageConsumer -> jmsTextMessageConsumer.accept(receivedMessage));

                if (receivedMessage != null) {
                    final var signal = extractSignalFromMessage(message, receivedMessage);
                    final var responseCorrelationId = signal.getDittoHeaders().getCorrelationId().orElse(null);
                    final var correlationId = message.getJMSCorrelationID();
                    if (responseCorrelationId == null || !responseCorrelationId.equals(correlationId)) {
                        LOGGER.warn("Received AMQP message but correlation ID not matching. Expected correlationId:" +
                                        " {} but was: {} Message: {}",
                                correlationId,
                                responseCorrelationId,
                                message);
                        return null;
                    }
                    if (signal instanceof CommandResponse) {
                        return (CommandResponse<?>) signal;
                    } else {
                        LOGGER.warn("Got unknown AMQP message: {}", signal);
                        return null;
                    }
                } else {
                    LOGGER.warn("No message received within the given timeout!");
                    return null;
                }
            } catch (final JMSException jmsException) {
                throw new IllegalStateException("JMSException during send", jmsException);
            }
        });
    }

    private JmsTextMessage tryToGetJmsTextMessage(final Adaptable adaptable, final OptionsMap options) {
        try {
            return getJmsTextMessage(adaptable, options);
        } catch (final JMSException e) {
            throw new IllegalStateException(
                    MessageFormat.format("Failed to get JmsTextMessage for Adaptable: {0}", e.getMessage()),
                    e
            );
        }
    }

    private JmsTextMessage getJmsTextMessage(final Adaptable adaptable, final OptionsMap options) throws JMSException {
        final var result = (JmsTextMessage) sessionDeviceToCloud.createTextMessage(getAdaptableAsJsonString(adaptable));
        result.setJMSReplyTo(new JmsQueue(replyAddress));
        result.setJMSCorrelationID(getCorrelationIdOrRandom(adaptable));

        tryToInjectHeadersIntoMessage(result, options);
        putMessageAnnotationHeader(result, options);
        injectContentTypeIntoMessage(adaptable, result);

        return result;
    }

    private static String getAdaptableAsJsonString(final Adaptable adaptable) {
        final var jsonifiableAdaptable = ProtocolFactory.wrapAsJsonifiableAdaptable(adaptable);
        return jsonifiableAdaptable.toJsonString();
    }

    private static String getCorrelationIdOrRandom(final WithDittoHeaders withDittoHeaders) {
        final var adaptableDittoHeaders = withDittoHeaders.getDittoHeaders();
        return adaptableDittoHeaders.getCorrelationId().orElseGet(() -> String.valueOf(UUID.randomUUID()));
    }

    @SuppressWarnings("unchecked")
    private static void tryToInjectHeadersIntoMessage(final TextMessage message, final OptionsMap optionsMap) {
        optionsMap.getValue(AmqpClientOptionDefinitions.Send.ADDITIONAL_AMQP_HEADERS)
                .map(map -> (Map<String, String>) map)
                .ifPresent(additionalHeaders -> tryToInjectHeadersIntoMessage(additionalHeaders, message));
    }

    private static void putMessageAnnotationHeader(final TextMessage message, final OptionsMap optionsMap) {
        optionsMap.getValue(AmqpClientOptionDefinitions.Send.MESSAGE_ANNOTATION_HEADER)
                .ifPresent(messageAnnotationHeader -> getAmqpJmsMessageFacade(message)
                        .ifPresent(amqpJmsMessageFacade -> amqpJmsMessageFacade.setTracingAnnotation(
                                messageAnnotationHeader.key(),
                                messageAnnotationHeader.value()
                        )));
    }

    public static Optional<AmqpJmsMessageFacade> getAmqpJmsMessageFacade(final TextMessage textMessage) {
        final Optional<AmqpJmsMessageFacade> result;
        if (textMessage instanceof JmsMessage jmsMessage) {
            if (jmsMessage.getFacade() instanceof AmqpJmsMessageFacade amqpJmsMessageFacade) {
                result = Optional.of(amqpJmsMessageFacade);
            } else {
                result = Optional.empty();
            }
        } else {
            result = Optional.empty();
        }
        return result;
    }

    /**
     * Sends the given response to the source queue.
     *
     * @param response the response to send.
     * @param <T> the type of the response.
     */
    public <T extends CommandResponse<T>> void sendResponse(final CommandResponse<T> response,
            final Option<?>... options) {

        final var correlationId = response
                .getDittoHeaders()
                .getCorrelationId()
                .orElseGet(() -> UUID.randomUUID().toString());

        final var adaptable = protocolAdapter.toAdaptable(response);
        final var stringMessage = ProtocolFactory.wrapAsJsonifiableAdaptable(adaptable).toJsonString();

        final var source = new JmsQueue(sourceAddress);

        try (final var messageProducer = sessionCloudToDevice.createProducer(source)) {
            final var message = sessionCloudToDevice.createTextMessage(stringMessage);
            message.setJMSCorrelationID(correlationId);

            tryToInjectHeadersIntoMessage(message, OptionsMap.of(options));
            injectContentTypeIntoMessage(adaptable, message);

            LOGGER.info("Sending message <{}> to <{}>.", stringMessage, messageProducer.getDestination());
            messageProducer.send(
                    message,
                    DeliveryMode.NON_PERSISTENT,
                    Message.DEFAULT_PRIORITY,
                    Message.DEFAULT_TIME_TO_LIVE
            );
        } catch (final JMSException jmsException) {
            throw new IllegalStateException("JMSException during send", jmsException);
        }

    }

    public void sendRaw(
            final String rawTextMessage,
            @Nullable final Map<String, String> rawHeaders,
            @Nullable final CharSequence correlationId,
            final Option<?>... options
    ) {
        final var source = new JmsQueue(sourceAddress);
        try (final var messageProducer = sessionCloudToDevice.createProducer(source)) {
            final var message = sessionCloudToDevice.createTextMessage(rawTextMessage);

            if (null != correlationId) {
                message.setJMSCorrelationID(correlationId.toString());
            }

            if (null != rawHeaders) {
                tryToInjectHeadersIntoMessage(rawHeaders, message);
                injectContentTypeIntoMessage(rawHeaders, message);
            }

            putMessageAnnotationHeader(message, OptionsMap.of(options));

            LOGGER.info("Sending raw message <{}> to <{}>.", rawTextMessage, messageProducer.getDestination());
            messageProducer.send(
                    message,
                    DeliveryMode.NON_PERSISTENT,
                    Message.DEFAULT_PRIORITY,
                    Message.DEFAULT_TIME_TO_LIVE
            );
        } catch (final JMSException e) {
            throw new IllegalStateException(MessageFormat.format("Failed to send raw message: {0}", e.getMessage()), e);
        }
    }

    /**
     * Sets the specified consumer for {@link TextMessage}s.
     * This replaces the previously set consumer.
     *
     * @param textMessageConsumer the consumer to be set.
     * @throws NullPointerException if {@code textMessageConsumer} is {@code null}.
     */
    public void onTextMessage(final Consumer<TextMessage> textMessageConsumer) {
        textMessageConsumerReference.set(checkNotNull(textMessageConsumer, "textMessageConsumer"));
    }

    /**
     * Replace the signal consumer for incoming signals.
     *
     * @param signalConsumer the signal consumer.
     * @throws NullPointerException if {@code signalConsumer} is {@code null}.
     */
    public void onSignal(final Consumer<Signal<?>> signalConsumer) {
        signalConsumerReference.set(checkNotNull(signalConsumer, "signalConsumer"));
    }

    private Signal<? extends Signal<?>> extractSignalFromMessage(final TextMessage message,
            final TextMessage receivedMessage) throws JMSException {

        final var messageText = receivedMessage.getText();
        LOGGER.info("Received message <{}>.", messageText);
        final var messageJson = Optional.of(JsonFactory.readFrom(messageText))
                .filter(JsonValue::isObject)
                .map(JsonValue::asObject)
                .orElseThrow(() -> new JsonParseException(
                        "The Amqp message was not a JSON object as required:\n" + message));

        // check headers of external messages before delivering them
        messageJson.getValue("headers")
                .map(JsonValue::asObject)
                .ifPresent(HeaderBlocklistChecker::assertHeadersDoNotIncludeBlocklisted);

        final var jsonifiableAdaptable = ProtocolFactory.jsonifiableAdaptableFromJson(messageJson);
        return protocolAdapter.fromAdaptable(jsonifiableAdaptable);
    }

    private static void injectContentTypeIntoMessage(
            final WithDittoHeaders withDittoHeaders,
            final TextMessage textMessage
    ) {
        final var dittoHeaders = withDittoHeaders.getDittoHeaders();
        injectContentTypeIntoMessage(dittoHeaders.asCaseSensitiveMap(), textMessage);
    }

    private static void injectContentTypeIntoMessage(final Map<String, String> headersMap, final TextMessage message) {
        getAmqpJmsMessageFacade(message)
                .ifPresent(amqpJmsMessageFacade -> amqpJmsMessageFacade.setContentType(
                        Symbol.getSymbol(headersMap.get("content-type"))
                ));
    }

    private static void tryToInjectHeadersIntoMessage(final Map<String, String> headersMap, final TextMessage message) {
        getAmqpJmsMessageFacade(message)
                .ifPresent(amqpJmsMessageFacade -> headersMap.forEach((key, value) -> {
                    try {
                        amqpJmsMessageFacade.setApplicationProperty(key, value);
                    } catch (final JMSException ex) {
                        LOGGER.warn("Could not set application-property <{}>: {}", key, jmsExceptionToString(ex));
                    }
                }));
    }

    @Override
    public CompletableFuture<CommandResponse<?>> send(final Command<?> command, final Option<?>... options) {
        return sendAdaptable(protocolAdapter.toAdaptable(command), options);
    }

    @Override
    public void disconnect() {
        try {
            disconnectDeviceToCloud();
            disconnectCloudToDevice();
            connection.close();
        } catch (final JMSException jmsException) {
            throw new IllegalStateException("JMSException during tearDown", jmsException);
        }
    }

    private void disconnectDeviceToCloud() throws JMSException {
        if (sessionDeviceToCloud != null) {
            sessionDeviceToCloud.close();
            sessionDeviceToCloud = null;
        }
    }

    private void disconnectCloudToDevice() throws JMSException {
        if (sessionCloudToDevice != null) {
            targetQueue = null;
            targetConsumer.close();
            sessionCloudToDevice.close();
            sessionCloudToDevice = null;
        }
    }

    private static String jmsExceptionToString(final JMSException jmsException) {
        if (jmsException.getCause() != null) {
            return String.format("[%s] %s (cause: %s - %s)", jmsException.getErrorCode(), jmsException.getMessage(),
                    jmsException.getCause().getClass().getSimpleName(), jmsException.getCause().getMessage());
        }

        return String.format("[%s] %s", jmsException.getErrorCode(), jmsException.getMessage());
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [" +
                " endpointUri=" + endpointUri +
                ", connectionName=" + connectionName +
                ", sourceAddress=" + sourceAddress +
                ", targetAddress=" + targetAddress +
                ", replyAddress=" + replyAddress +
                " ]";
    }

}
