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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.qpid.jms.JmsQueue;
import org.apache.qpid.jms.message.JmsMessage;
import org.apache.qpid.jms.message.facade.JmsMessageFacade;
import org.apache.qpid.jms.provider.amqp.message.AmqpJmsMessageFacade;
import org.apache.qpid.proton.amqp.Symbol;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.connectivity.api.ExternalMessage;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityWorker;
import org.slf4j.Logger;

public final class Amqp10ConnectivityWorker
        extends AbstractConnectivityWorker<BlockingQueue<Message>, Message> {

    private final JmsMessageListeners jmsMessageListeners;
    private final JmsSenders jmsSenders;
    private final JmsReceivers jmsReceivers;
    private final JmsSessions jmsSessions;
    private final TargetAddressBuilder targetAddressBuilder;
    private final SourceAddressBuilder sourceAddressBuilder;
    private final Duration waitTimeout;

    public Amqp10ConnectivityWorker(final Logger logger,
            final JmsMessageListeners jmsMessageListeners,
            final JmsSenders jmsSenders,
            final JmsReceivers jmsReceivers,
            final JmsSessions jmsSessions,
            final TargetAddressBuilder targetAddressBuilder,
            final SourceAddressBuilder sourceAddressBuilder,
            final Duration waitTimeout) {
        super(logger);
        this.jmsMessageListeners = jmsMessageListeners;
        this.jmsSenders = jmsSenders;
        this.jmsReceivers = jmsReceivers;
        this.jmsSessions = jmsSessions;
        this.targetAddressBuilder = targetAddressBuilder;
        this.sourceAddressBuilder = sourceAddressBuilder;
        this.waitTimeout = waitTimeout;
    }


    public MessageProducer createSender(final String connectionId, final String sourceAddress) throws JMSException {
        logger.info("Creating Sender for connection <{}> with source address <{}>", connectionId, sourceAddress);
        return Optional.ofNullable(resolveSession(connectionId))
                .orElseThrow(() -> new IllegalStateException("No active JMS session for id: " + connectionId))
                .createProducer(null);
//                .createProducer(new JmsQueue(sourceAddress));
    }

    public MessageConsumer createReceiver(final String connectionId, final String targetAddress)
            throws JMSException {

        logger.info("Creating Receiver for connection <{}> with target address <{}>", connectionId, targetAddress);
        final MessageConsumer consumer = Optional.ofNullable(resolveSession(connectionId))
                .orElseThrow(() -> new IllegalStateException(
                        "No active JMS session for id: " + connectionId + " in " + jmsSessions.get().keySet()))
                .createConsumer(new JmsQueue(targetAddress));
        consumer.setMessageListener(msg -> {
            final MessageListener targetHandler = jmsMessageListeners.get().get(targetAddress);
            if (targetHandler != null) {
                targetHandler.onMessage(msg);
                try {
                    if (msg instanceof TextMessage) {
                        final String payload = ((TextMessage) msg).getText();
                        logger.info(
                                "connection <{}> received text message with correlation-id <{}> and content: {}",
                                connectionId, msg.getJMSCorrelationID(), payload);
                    } else if (msg instanceof final BytesMessage bytesMessage) {
                        final long bodyLength = bytesMessage.getBodyLength();
                        logger.info(
                                "connection <{}> received bytes message with correlation-id <{}> and content length: {}",
                                connectionId, msg.getJMSCorrelationID(), bodyLength);
                    }
                } catch (final JMSException e) {
                    throw new IllegalStateException("Got JMSException during createConsumer in MessageListener", e);
                }
            } else {
                logger.warn("Target handler was missing for connection <{}>", connectionId);
            }
        });
        return consumer;
    }

    @Override
    public BlockingQueue<Message> initResponseConsumer(final String connectionName, final String routingKey) {

        final BlockingQueue<Message> messageQueue = new LinkedBlockingDeque<>();
        try {
            final MessageConsumer messageConsumer =
                    jmsSessions.get().get(connectionName).createConsumer(new JmsQueue(routingKey));
            messageConsumer.setMessageListener(msg -> {
                if (msg instanceof TextMessage) {
                    try {
                        final String payload = ((TextMessage) msg).getText();
                        logger.info("Received response message with correlation-id <{}> and content: {}",
                                msg.getJMSCorrelationID(), payload);
                    } catch (final JMSException e) {
                        throw new IllegalStateException("Error during JMS TextMessage parsing", e);
                    }
                    messageQueue.add(msg);
                } else if (msg instanceof BytesMessage) {
                    try {
                        logger.info("Received response message with correlation-id <{}> and byte payload.",
                                msg.getJMSCorrelationID());
                    } catch (final JMSException e) {
                        throw new IllegalStateException("Error during JMS TextMessage parsing", e);
                    }
                    messageQueue.add(msg);
                }
            });
        } catch (final JMSException e) {
            logger.error("Error during creating response consumer", e);
        }

        return messageQueue;
    }

    @Override
    protected BlockingQueue<Message> initTargetsConsumer(final String connectionName) {
        return initTargetsConsumer(connectionName, targetAddressBuilder.createAddress(connectionName));
    }

    @Override
    protected BlockingQueue<Message> initTargetsConsumer(final String connectionName, final String targetAddress) {

        // force override of message handler for the target address receiver
        logger.info("Registering MessageHandler for target messages of connection <{}> at <{}>", connectionName,
                targetAddress);
        final BlockingQueue<Message> messageQueue = new LinkedBlockingDeque<>();
        jmsMessageListeners.get().put(targetAddress, messageQueue::add);
        jmsReceivers.get().computeIfAbsent(targetAddress, addr -> {
            try {
                return createReceiver(connectionName, targetAddress);
            } catch (final Throwable error) {
                throw new RuntimeException(error);
            }
        });

        return messageQueue;
    }

    @Override
    public Message consumeResponse(final String correlationId, final BlockingQueue<Message> consumer) {
        logger.info("Waiting for AMQP1.0 CommandResponse on routingKey <{}>", correlationId);
        try {
            return consumer.poll(waitTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected CompletableFuture<Message> consumeResponseInFuture(final String correlationId,
            final BlockingQueue<Message> consumer) {
        logger.info("Waiting asynchronously for AMQP1.0 CommandResponse on routingKey <{}>", correlationId);

        return CompletableFuture.supplyAsync(() -> {
            try {
                return consumer.poll(waitTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                return null;
            }
        });
    }

    @Override
    protected Message consumeFromTarget(final String connectionName, final BlockingQueue<Message> targetsConsumer) {
        logger.info("Waiting for AMQP1.0 message on routingKey <{}>",
                targetAddressBuilder.createAddress(connectionName));

        try {
            return targetsConsumer.poll(waitTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected CompletableFuture<Message> consumeFromTargetInFuture(final String connectionName,
            final BlockingQueue<Message> targetsConsumer) {
        logger.info("Waiting asynchronously for AMQP1.0 message on routingKey <{}>",
                targetAddressBuilder.createAddress(connectionName));

        return CompletableFuture.supplyAsync(() -> {
            try {
                return targetsConsumer.poll(waitTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                return null;
            }
        });
    }

    @Override
    protected String getCorrelationId(final Message message) {
        try {
            return message.getJMSCorrelationID();
        } catch (final JMSException e) {
            throw new IllegalStateException("Could not get correlationId from JMS message", e);
        }
    }

    @Override
    public void sendAsJsonString(final String destination, final String correlationId,
            final String stringMessage, final Map<String, String> headersMap) {

        final String address = sourceAddressBuilder.createAddress(destination);
        final TextMessage message;
        try {
            final Session session = resolveSession(destination);
            message = session.createTextMessage(stringMessage);
            message.setJMSDestination(new JmsQueue(address));
            message.setJMSCorrelationID(correlationId);
            message.setJMSReplyTo(new JmsQueue(correlationId));
        } catch (final JMSException e) {
            throw new IllegalStateException("Error during creating new JMS TextMessage", e);
        }

        if (message instanceof JmsMessage) {
            final JmsMessageFacade facade = ((JmsMessage) message).getFacade();
            if (facade instanceof final AmqpJmsMessageFacade amqpJmsMessageFacade) {
                headersMap.forEach((key, value) -> {
                    try {
                        amqpJmsMessageFacade.setApplicationProperty(key, value);
                    } catch (final JMSException ex) {
                        logger.warn("Could not set application-property <{}>: {}",
                                key, jmsExceptionToString(ex));
                    }
                });
            }
        }

        Optional.ofNullable(headersMap.get("content-type")).ifPresent(ct -> {
            if (message instanceof JmsMessage) {
                final JmsMessageFacade facade = ((JmsMessage) message).getFacade();
                if (facade instanceof AmqpJmsMessageFacade) {
                    ((AmqpJmsMessageFacade) facade).setContentType(Symbol.getSymbol(ct));
                }
            }
        });

        logger.info("Sending AMQP1.0 message to address <{}> with correlation-id <{}>: {}", address, correlationId,
                stringMessage);
        sendAmqp10Message(destination, message);
    }

    @Override
    protected void sendAsBytePayload(final String destination, final String correlationId, final byte[] byteMessage,
            final Map<String, String> headersMap) {
        final String address = sourceAddressBuilder.createAddress(destination);
        final BytesMessage message;
        try {
            final Session session = resolveSession(destination);
            message = session.createBytesMessage();
            message.writeBytes(byteMessage);
            message.setJMSDestination(new JmsQueue(address));
            message.setJMSCorrelationID(correlationId);
            message.setJMSReplyTo(new JmsQueue(correlationId));
        } catch (final JMSException e) {
            throw new IllegalStateException("Error during creating new JMS TextMessage", e);
        }

        if (message instanceof JmsMessage) {
            final JmsMessageFacade facade = ((JmsMessage) message).getFacade();
            if (facade instanceof final AmqpJmsMessageFacade amqpJmsMessageFacade) {
                headersMap.forEach((key, value) -> {
                    try {
                        amqpJmsMessageFacade.setApplicationProperty(key, value);
                    } catch (final JMSException ex) {
                        logger.warn("Could not set application-property <{}>: {}",
                                key, jmsExceptionToString(ex));
                    }
                });
            }
        }

        Optional.ofNullable(headersMap.get("content-type")).ifPresent(ct -> {
            if (message instanceof JmsMessage) {
                final JmsMessageFacade facade = ((JmsMessage) message).getFacade();
                if (facade instanceof AmqpJmsMessageFacade) {
                    ((AmqpJmsMessageFacade) facade).setContentType(Symbol.getSymbol(ct));
                }
            }
        });

        logger.info("Sending AMQP1.0 message to address <{}> with correlation-id <{}>: {}", address, correlationId,
                byteMessage);
        sendAmqp10Message(destination, message);
    }

    void sendWithEmptyBytePayload(final String destination, final String correlationId,
            final Map<String, String> headersMap) {

        final String address = sourceAddressBuilder.createAddress(destination);
        final BytesMessage message;
        try {
            final Session session = resolveSession(destination);
            message = session.createBytesMessage();
            message.setJMSDestination(new JmsQueue(address));
            message.setJMSCorrelationID(correlationId);
            message.setJMSReplyTo(new JmsQueue(correlationId));
        } catch (final JMSException e) {
            throw new IllegalStateException("Error during creating new JMS BytesMessage", e);
        }

        if (message instanceof JmsMessage) {
            final JmsMessageFacade facade = ((JmsMessage) message).getFacade();
            if (facade instanceof final AmqpJmsMessageFacade amqpJmsMessageFacade) {
                headersMap.forEach((key, value) -> {
                    try {
                        amqpJmsMessageFacade.setApplicationProperty(key, value);
                    } catch (final JMSException ex) {
                        logger.warn("Could not set application-property <{}>: {}",
                                key, jmsExceptionToString(ex));
                    }
                });
            }
        }

        Optional.ofNullable(headersMap.get("content-type")).ifPresent(ct -> {
            if (message instanceof JmsMessage) {
                final JmsMessageFacade facade = ((JmsMessage) message).getFacade();
                if (facade instanceof AmqpJmsMessageFacade) {
                    ((AmqpJmsMessageFacade) facade).setContentType(Symbol.getSymbol(ct));
                }
            }
        });

        logger.info("Sending AMQP1.0 message with empty byte payload to address <{}> with correlation-id <{}>", address, correlationId);
        sendAmqp10Message(destination, message);

    }

    private Session resolveSession(final String destination) {
        final int indexOfSuffixSeparator = destination.lastIndexOf("_");
        final Session session = jmsSessions.get().get(destination);
        if (session != null) {
            return session;
        } else if (indexOfSuffixSeparator > 0) {
            final String destinationWithoutSuffix = destination.substring(0, indexOfSuffixSeparator);
            return jmsSessions.get().get(destinationWithoutSuffix);
        }
        return null;
    }

    private MessageProducer resolveSender(final String destination) {
        final int indexOfSuffixSeparator = destination.lastIndexOf("_");
        final MessageProducer sender = jmsSenders.get().get(destination);
        if (sender != null) {
            return sender;
        } else if (indexOfSuffixSeparator > 0) {
            final String destinationWithoutSuffix = destination.substring(0, indexOfSuffixSeparator);
            return jmsSenders.get().get(destinationWithoutSuffix);
        }
        return null;
    }

    private static String jmsExceptionToString(final JMSException jmsException) {
        if (jmsException.getCause() != null) {
            return String.format("[%s] %s (cause: %s - %s)", jmsException.getErrorCode(), jmsException.getMessage(),
                    jmsException.getCause().getClass().getSimpleName(), jmsException.getCause().getMessage());
        }

        return String.format("[%s] %s", jmsException.getErrorCode(), jmsException.getMessage());
    }


    private void sendAmqp10Message(final String destination, final Message amqpMessage) {
        final MessageProducer sender = resolveSender(destination);
        sendAmqp10Message(sender, amqpMessage, 5); // 5 retries by default
    }

    private void sendAmqp10Message(@Nullable final MessageProducer sender,
            final Message amqpMessage,
            final int retryCounter) {
        if (retryCounter <= 0) {
            logger.error("Could not send message after <{}> retries: <{}>", retryCounter, amqpMessage);
        } else {
            if (sender != null) {
                try {
                    sender.send(amqpMessage.getJMSDestination(), amqpMessage);

                } catch (final Exception e) {
                    logger.error("Got exception during AMQP sending - retrying <{}> more times", retryCounter, e);
                    // retry by recurse! ]:->
                    sendAmqp10Message(sender, amqpMessage, retryCounter - 1);
                }
            } else {
                logger.error("Missing sender -> could not send message <{}>", amqpMessage);
            }
        }

    }

    @Override
    protected JsonifiableAdaptable jsonifiableAdaptableFrom(final Message message) {
        return jsonifiableAdaptableFrom(textFrom(message));
    }

    @Override
    protected String textFrom(final Message message) {
        if (message instanceof TextMessage) {
            try {
                return ((TextMessage) message).getText();
            } catch (final JMSException e) {
                logger.error("Error when getting text from JMS TextMessage", e);
                throw new IllegalStateException("Error when getting text from JMS TextMessage", e);
            }
        } else {
            throw new AssertionError("Message type was unknown: " + message);
        }
    }

    @Override
    protected byte[] bytesFrom(final Message message) {
        if (message instanceof BytesMessage) {
            try {
                final BytesMessage bytesMessage = (BytesMessage) message;
                final long bodyLength = bytesMessage.getBodyLength();
                if (bodyLength >= Integer.MIN_VALUE && bodyLength <= Integer.MAX_VALUE) {
                    final int length = (int) bodyLength;
                    final ByteBuffer byteBuffer = ByteBuffer.allocate(length);
                    bytesMessage.readBytes(byteBuffer.array());
                    return byteBuffer.array();
                } else {
                    throw new IllegalArgumentException("Message too large...");
                }
            } catch (final JMSException e) {
                logger.error("Error when getting text from JMS TextMessage", e);
                throw new IllegalStateException("Error when getting text from JMS TextMessage", e);
            }
        } else {
            throw new AssertionError("Message type was unknown: " + message);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Map<String, String> checkHeaders(final Message message,
            final Consumer<JsonObject> verifyBlockedHeaders) {
        try {
            final Map<String, String> headers = extractHeadersMapFromJmsMessage((JmsMessage) message);

            final Object correlationId = message.getJMSCorrelationID();
            if (correlationId != null) {
                // AMQP1.0 via JMS specific: the "correlation-id" is removed from applicationProperties, and is in the protocol:
                headers.put("correlation-id", correlationId.toString());
            }

            final JmsMessageFacade facade = ((JmsMessage) message).getFacade();
            final Symbol ct = ((AmqpJmsMessageFacade) facade).getContentType();
            final String contentType = ct != null ? ct.toString() : null;
            if (contentType != null) {
                // AMQP1.0 via JMS specific: the "content-type" is removed from applicationProperties, and is in the protocol:
                headers.put("content-type", contentType);
            }

            verifyBlockedHeaders.accept(mapToJsonObject(headers));
            // the "subject" is required in test ensureConsumedMessagesWithHeaderMappingAppliedContainExpectedHeaders()
            // that's why we do the assert before adding the "subject" below from the JMS message:

            final String subject = message.getJMSType();
            if (subject != null) {
                // AMQP1.0 via JMS specific: the "subject" is removed from applicationProperties, and is in the protocol:
                headers.put("subject", subject);
            }
            return headers;
        } catch (final JMSException e) {
            throw new IllegalStateException("Error during parsing header of JMS message", e);
        }
    }

    private Map<String, String> extractHeadersMapFromJmsMessage(final JmsMessage message) throws JMSException {

        final Map<String, String> headersFromJmsProperties;

        final JmsMessageFacade facade = message.getFacade();
        if (facade instanceof final AmqpJmsMessageFacade amqpJmsMessageFacade) {
            final Set<String> names =
                    amqpJmsMessageFacade.getApplicationPropertyNames(amqpJmsMessageFacade.getPropertyNames());
            headersFromJmsProperties = new HashMap<>(names.stream()
                    .map(key -> getPropertyAsEntry(amqpJmsMessageFacade, key))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

            final Symbol contentType = amqpJmsMessageFacade.getContentType();
            if (null != contentType) {
                headersFromJmsProperties.put(ExternalMessage.CONTENT_TYPE_HEADER, contentType.toString());
            }
        } else {
            throw new JMSException("Message facade was not of type AmqpJmsMessageFacade");
        }

        final String replyTo = message.getJMSReplyTo() != null ? String.valueOf(message.getJMSReplyTo()) : null;
        if (replyTo != null) {
            headersFromJmsProperties.put(ExternalMessage.REPLY_TO_HEADER, replyTo);
        }

        final String jmsCorrelationId = message.getJMSCorrelationID() != null ? message.getJMSCorrelationID() :
                message.getJMSMessageID();
        if (jmsCorrelationId != null) {
            headersFromJmsProperties.put(DittoHeaderDefinition.CORRELATION_ID.getKey(), jmsCorrelationId);
        }

        return headersFromJmsProperties;
    }

    @Nullable
    private Map.Entry<String, String> getPropertyAsEntry(final AmqpJmsMessageFacade message, final String key) {
        try {
            final Object applicationProperty = message.getApplicationProperty(key);
            if (applicationProperty != null) {
                return new AbstractMap.SimpleImmutableEntry<>(key, applicationProperty.toString());
            } else {
                logger.debug("Property '{}' was null", key);
                return null;
            }
        } catch (final JMSException e) {
            logger.debug("Property '{}' could not be read, dropping...", key);
            return null;
        }
    }

    @Override
    protected BrokerType getBrokerType() {
        return BrokerType.AMQP10;
    }

    @FunctionalInterface
    public interface TargetAddressBuilder {

        String createAddress(final String connectionId);
    }

    @FunctionalInterface
    public interface SourceAddressBuilder {

        String createAddress(final String connectionId);
    }

    @FunctionalInterface
    public interface JmsMessageListeners {

        ConcurrentMap<String, MessageListener> get();
    }

    @FunctionalInterface
    public interface JmsSenders {

        ConcurrentMap<String, MessageProducer> get();
    }

    @FunctionalInterface
    public interface JmsSessions {

        ConcurrentMap<String, Session> get();
    }

    @FunctionalInterface
    public interface JmsReceivers {

        ConcurrentMap<String, MessageConsumer> get();
    }

}
