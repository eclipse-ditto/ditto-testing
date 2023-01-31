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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityWorker;
import org.slf4j.Logger;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;

public final class RabbitMqConnectivityWorker
        extends AbstractConnectivityWorker<QueueingConsumer, QueueingConsumer.Delivery> {

    private final Supplier<Channel> channelSupplier;
    private final Function<String, String> targetAddressBuilder;
    private final Duration waitTimeout;

    private final String inboundExchange;
    private final String outboundRoutingKey;
    private final String sourceAddress;

    RabbitMqConnectivityWorker(final Logger logger,
            final Supplier<Channel> channelSupplier,
            final Function<String, String> targetAddressBuilder,
            final Duration waitTimeout,
            final String inboundExchange,
            final String outboundRoutingKey,
            final String sourceAddress) {
        super(logger);
        this.channelSupplier = channelSupplier;
        this.targetAddressBuilder = targetAddressBuilder;
        this.waitTimeout = waitTimeout;
        this.inboundExchange = inboundExchange;
        this.outboundRoutingKey = outboundRoutingKey;
        this.sourceAddress = sourceAddress;
    }

    @Override
    protected QueueingConsumer initResponseConsumer(final String connectionName,
            final String routingKey) {
        final Channel channel = channelSupplier.get();
        final QueueingConsumer consumer = new QueueingConsumer(channel);
        try {
            channel.queueDeclare(routingKey, false, false, true, null);
            channel.queueBind(routingKey, inboundExchange, routingKey);
            channel.basicConsume(routingKey, false, consumer);
        } catch (final IOException e) {
            throw mapException(e);
        }

        return consumer;
    }

    @Override
    protected QueueingConsumer initTargetsConsumer(final String connectionName) {
        return initTargetsConsumer(connectionName, targetAddressBuilder.apply(connectionName));
    }

    @Override
    protected QueueingConsumer initTargetsConsumer(final String connectionName, final String targetAddress) {
        final Channel channel = channelSupplier.get();
        final QueueingConsumer consumer = new QueueingConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                    byte[] body) throws IOException {
                logger.info("rabbitmqClient: got message with properties <{}>: {}", properties, envelope);
                super.handleDelivery(consumerTag, envelope, properties, body);
            }
        };
        try {
            logger.info("Consuming from queue <{}>", targetAddress);
            channel.basicConsume(targetAddress, false, consumer);
        } catch (final IOException e) {
            throw mapException(e);
        }

        return consumer;
    }

    @Override
    protected QueueingConsumer.Delivery consumeResponse(final String correlationId, final QueueingConsumer consumer) {
        logger.info("Waiting for RabbitMQ CommandResponse on routingKey <{}>", correlationId);

        return doConsumeDelivery(consumer, correlationId, null);
    }

    @Override
    protected CompletableFuture<QueueingConsumer.Delivery> consumeResponseInFuture(final String correlationId,
            final QueueingConsumer consumer) {
        logger.info("Waiting asynchronously for CommandResponse Event on routingKey <{}>", correlationId);

        return CompletableFuture.supplyAsync(() -> doConsumeDelivery(consumer, correlationId, null));
    }

    @Override
    protected QueueingConsumer.Delivery consumeFromTarget(final String connectionName,
            final QueueingConsumer targetsConsumer) {
        final String routingKey = outboundRoutingKey + connectionName;
        logger.info("Waiting for RabbitMQ message on routingKey <{}>", routingKey);

        return doConsumeDelivery(targetsConsumer, routingKey, connectionName);
    }

    @Override
    protected CompletableFuture<QueueingConsumer.Delivery> consumeFromTargetInFuture(final String connectionName,
            final QueueingConsumer targetsConsumer) {
        final String routingKey = outboundRoutingKey + connectionName;
        logger.info("Waiting asynchronously for RabbitMQ message on routingKey <{}>", routingKey);

        return CompletableFuture.supplyAsync(() -> doConsumeDelivery(targetsConsumer, routingKey, connectionName));
    }

    private QueueingConsumer.Delivery doConsumeDelivery(final QueueingConsumer consumer, final String routingKey,
            @Nullable final String connectionName) {
        final QueueingConsumer.Delivery delivery = nextDelivery(consumer, waitTimeout);
        if (delivery == null) {
            logger.warn("Timed out waiting for delivery of connection <{}> on routingKey <{}> after <{}>",
                    connectionName, routingKey, waitTimeout);
            return null;
        } else {
            logger.info("Got delivery of connection <{}> with correlation-id <{}>, acking..", connectionName,
                    delivery.getProperties().getCorrelationId());
            basicAck(delivery.getEnvelope().getDeliveryTag());
            return delivery;
        }
    }

    @Override
    protected String getCorrelationId(final QueueingConsumer.Delivery delivery) {
        return delivery.getProperties().getCorrelationId();
    }

    @Override
    protected void sendAsJsonString(final String connectionName, final String correlationId,
            final String stringMessage, final Map<String, String> headersMap) {

        final AMQP.BasicProperties.Builder mqBasicPropertiesBuilder = new AMQP.BasicProperties()
                .builder()
                .correlationId(correlationId)
                .replyTo(correlationId)
                .contentType(headersMap.get("content-type"))
                .headers(Collections.unmodifiableMap(headersMap));

        final AMQP.BasicProperties mqBasicProperties = mqBasicPropertiesBuilder.build();

        final String routingKey = sourceAddress + connectionName;
        logger.info("Sending RabbitMQ message to routingKey <{}> with correlation-id <{}> and headers {}: {}",
                routingKey, correlationId, headersMap, stringMessage);
        try {
            final byte[] bytes = stringMessage.getBytes(StandardCharsets.UTF_8);
            channelSupplier.get().basicPublish(inboundExchange, routingKey, mqBasicProperties, bytes);
        } catch (final IOException e) {
            throw mapException(e);
        }
    }

    @Override
    protected void sendAsBytePayload(final String connectionName, final String correlationId, final byte[] byteMessage,
            final Map<String, String> headersMap) {

        final AMQP.BasicProperties.Builder mqBasicPropertiesBuilder = new AMQP.BasicProperties()
                .builder()
                .correlationId(correlationId)
                .replyTo(correlationId)
                .contentType(headersMap.get("content-type"))
                .headers(Collections.unmodifiableMap(headersMap));

        final AMQP.BasicProperties mqBasicProperties = mqBasicPropertiesBuilder.build();

        final String routingKey = sourceAddress + connectionName;
        logger.info("Sending RabbitMQ message to routingKey <{}> with correlation-id <{}> and headers {}: {}",
                routingKey, correlationId, headersMap, byteMessage);
        try {
            channelSupplier.get().basicPublish(inboundExchange, routingKey, mqBasicProperties, byteMessage);
        } catch (final IOException e) {
            throw mapException(e);
        }
    }

    @Override
    protected JsonifiableAdaptable jsonifiableAdaptableFrom(final QueueingConsumer.Delivery delivery) {
        final String body = new String(delivery.getBody());
        logger.info("Received RabbitMQ delivery with correlation-id <{}> and body: {}",
                getCorrelationId(delivery), body);

        return jsonifiableAdaptableFrom(body);
    }

    @Override
    protected String textFrom(final QueueingConsumer.Delivery delivery) {
        return new String(delivery.getBody());
    }

    @Override
    protected byte[] bytesFrom(final QueueingConsumer.Delivery message) {
        return message.getBody();
    }

    @Override
    protected Map<String, String> checkHeaders(final QueueingConsumer.Delivery message,
            final Consumer<JsonObject> verifyBlockedHeaders) {
        final Map<String, Object> adjustedHeaders = new HashMap<>(message.getProperties().getHeaders());

        final Map<String, String> headers = new HashMap<>(adjustedHeaders.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString())));

        verifyBlockedHeaders.accept(mapToJsonObject(headers));

        return headers;
    }

    @Override
    protected BrokerType getBrokerType() {
        return BrokerType.RABBITMQ;
    }


    private QueueingConsumer.Delivery nextDelivery(final QueueingConsumer consumer, final Duration timeout) {
        final QueueingConsumer.Delivery delivery;
        try {
            delivery = consumer.nextDelivery(timeout.toMillis());
        } catch (final InterruptedException e) {
            throw mapException(e);
        }
        return delivery;
    }

    private void basicAck(final long deliveryTag) {
        try {
            channelSupplier.get().basicAck(deliveryTag, false);
        } catch (final IOException e) {
            throw mapException(e);
        }
    }


}
