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
package org.eclipse.ditto.testing.system.connectivity.mqtt5;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.awaitility.core.ThrowingRunnable;
import org.eclipse.ditto.base.model.common.ByteBufferUtils;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.connectivity.api.ExternalMessage;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonObjectBuilder;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityWorker;
import org.slf4j.Logger;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishBuilder;

import org.apache.pekko.util.ByteString;

public final class Mqtt5ConnectivityWorker
        extends AbstractConnectivityWorker<BlockingQueue<Mqtt5Publish>, Mqtt5Publish> {

    private final TargetTopicSupplier topicSupplier;
    private final Supplier<Mqtt5AsyncClient> mqttClientSupplier;
    private final String connectionIdWithEnforcement;
    private final Duration waitTimeout;

    Mqtt5ConnectivityWorker(final Logger logger,
            final TargetTopicSupplier topicSupplier,
            final Supplier<Mqtt5AsyncClient> mqttClientSupplier,
            final String connectionIdWithEnforcement, final Duration waitTimeout) {

        super(logger);
        this.topicSupplier = topicSupplier;
        this.mqttClientSupplier = mqttClientSupplier;
        this.connectionIdWithEnforcement = connectionIdWithEnforcement;
        this.waitTimeout = waitTimeout;
    }

    private static String getPublishTopic(final String connectionId) {
        return connectionId + "/source";
    }

    private static void rethrow(final ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected BlockingQueue<Mqtt5Publish> initResponseConsumer(final String connectionName,
            final String correlationId) {

        return subscribe(correlationId);
    }

    @Override
    protected BlockingQueue<Mqtt5Publish> initTargetsConsumer(final String connectionName) {
        return subscribe(topicSupplier.getTopic(connectionName));
    }

    @Override
    protected BlockingQueue<Mqtt5Publish> initTargetsConsumer(final String connectionName, final String targetAddress) {
        return subscribe(targetAddress);
    }

    @Override
    protected Mqtt5Publish consumeResponse(final String correlationId, final BlockingQueue<Mqtt5Publish> consumer) {
        final AtomicReference<Mqtt5Publish> messageBox = new AtomicReference<>();
        rethrow(() -> messageBox.set(consumer.poll(waitTimeout.toMillis(), TimeUnit.MILLISECONDS)));
        return messageBox.get();
    }

    @Override
    protected CompletableFuture<Mqtt5Publish> consumeResponseInFuture(final String correlationId,
            final BlockingQueue<Mqtt5Publish> consumer) {

        return CompletableFuture.supplyAsync(() -> consumeResponse(correlationId, consumer));
    }

    @Override
    protected Mqtt5Publish consumeFromTarget(final String connectionName,
            final BlockingQueue<Mqtt5Publish> targetsConsumer) {

        return consumeResponse(connectionName, targetsConsumer);
    }

    @Override
    protected CompletableFuture<Mqtt5Publish> consumeFromTargetInFuture(final String connectionName,
            final BlockingQueue<Mqtt5Publish> targetsConsumer) {

        return consumeResponseInFuture(connectionName, targetsConsumer);
    }

    @Override
    protected String getCorrelationId(final Mqtt5Publish message) {
        return message.getCorrelationData().map(ByteBufferUtils::toUtf8String).orElse("unknown-correlation-id");
    }

    @Override
    protected void sendAsJsonString(final String connectionName, final String correlationId, final String stringMessage,
            final Map<String, String> headersMap) {

        final Map<String, String> extraHeaders = new HashMap<>(headersMap);
        extraHeaders.put(ExternalMessage.REPLY_TO_HEADER, correlationId);
        if (!headersMap.containsKey(DittoHeaderDefinition.CORRELATION_ID.getKey())) {
            extraHeaders.put(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId);
        }
        final String thingIdFromHeader = extraHeaders.getOrDefault("device_id", "invalid:id");
        final String mqttTopic;
        if (connectionIdWithEnforcement.equals(connectionName)) {
            // we need the thingId in the topic for enforcement
            mqttTopic = getPublishTopic(connectionName) + "/" + thingIdFromHeader.replaceFirst(Pattern.quote(":"), "/");
        } else {
            mqttTopic = getPublishTopic(connectionName);
        }
        final ByteString bytePayload = ByteString.fromString(stringMessage);
        rethrow(() -> {
            logger.info("mqttClient: publishing on topic <{}>: {}", mqttTopic, stringMessage);
            setContentType(mqttClientSupplier.get()
                    .publishWith()
                    .topic(mqttTopic)
                    .qos(MqttQos.EXACTLY_ONCE)
                    .payload(bytePayload.toByteBuffer())
                    .responseTopic(resolveResponseTopic(headersMap, extraHeaders))
                    .correlationData(ByteBufferUtils.fromUtf8String(correlationId))
                    .userProperties(Mqtt5UserProperties.of(extraHeaders.entrySet()
                            .stream()
                            .map(entry -> Mqtt5UserProperty.of(entry.getKey(), entry.getValue()))
                            .collect(Collectors.toList()))), extraHeaders).send();
        });
    }

    private String resolveResponseTopic(final Map<String, String> headersMap, final Map<String, String> extraHeaders) {
        String extraHeadersReplyTo = extraHeaders.get(ExternalMessage.REPLY_TO_HEADER);
        return Optional.ofNullable(headersMap.get(ExternalMessage.REPLY_TO_HEADER))
                .orElse(stripWildcardFromReplyTopic(extraHeadersReplyTo));
    }

    private static String stripWildcardFromReplyTopic(final String extraHeadersReplyTo) {
        return extraHeadersReplyTo.contains("#") ?
                extraHeadersReplyTo.substring(0, extraHeadersReplyTo.indexOf('#')) : extraHeadersReplyTo;
    }

    private Mqtt5PublishBuilder.Send.Complete<?> setContentType(final Mqtt5PublishBuilder.Send.Complete<?> send,
            final Map<String, String> headers) {

        if (headers.containsKey(DittoHeaderDefinition.CONTENT_TYPE.getKey())) {
            return send.contentType(headers.get(DittoHeaderDefinition.CONTENT_TYPE.getKey()));
        } else {
            return send;
        }
    }

    private static JsonObject packHeaders(final JsonObject payload, final Map<String, String> otherHeaders) {
        final String headers = "headers";
        final JsonObjectBuilder headersBuilder = payload.getValue(headers)
                .filter(JsonValue::isObject)
                .map(JsonValue::asObject)
                .map(JsonObject::toBuilder)
                .orElseGet(JsonFactory::newObjectBuilder);
        otherHeaders.forEach(headersBuilder::set);
        return payload.setValue(headers, headersBuilder.build());
    }

    @Override
    protected void sendAsBytePayload(final String connectionName, final String correlationId, final byte[] byteMessage,
            final Map<String, String> headersMap) {

        final Map<String, String> extraHeaders = new HashMap<>(headersMap);
        extraHeaders.put(ExternalMessage.REPLY_TO_HEADER, correlationId);
        final String thingIdFromHeader = extraHeaders.getOrDefault("device_id", "invalid:id");

        final String mqttTopic;
        if (connectionIdWithEnforcement.equals(connectionName)) {
            // we need the thingId in the topic for enforcement
            mqttTopic = getPublishTopic(connectionName) + "/" + thingIdFromHeader.replaceFirst(Pattern.quote(":"), "/");
        } else {
            mqttTopic = getPublishTopic(connectionName);
        }
        rethrow(() -> {
            logger.info("mqttClient: publishing on topic <{}>: {}", mqttTopic, byteMessage);
            setContentType(mqttClientSupplier.get()
                    .publishWith()
                    .topic(mqttTopic)
                    .qos(MqttQos.EXACTLY_ONCE)
                    .payload(byteMessage)
                    .responseTopic(correlationId)
                    .correlationData(ByteBufferUtils.fromUtf8String(correlationId))
                    .userProperties(
                            Mqtt5UserProperties.of(extraHeaders.entrySet()
                                    .stream()
                                    .map(entry -> Mqtt5UserProperty.of(entry.getKey(), entry.getValue()))
                                    .collect(Collectors.toList()))), extraHeaders).send();

        });
    }

    @Override
    protected JsonifiableAdaptable jsonifiableAdaptableFrom(final Mqtt5Publish message) {
        return jsonifiableAdaptableFrom(textFrom(message));
    }

    @Override
    protected String textFrom(final Mqtt5Publish message) {
        return new String(message.getPayloadAsBytes());
    }

    @Override
    protected byte[] bytesFrom(final Mqtt5Publish message) {
        return message.getPayloadAsBytes();
    }

    @Override
    protected Map<String, String> checkHeaders(final Mqtt5Publish message,
            final Consumer<JsonObject> verifyBlockedHeaders) {

        final var stringMap =
                message.getUserProperties()
                        .asList()
                        .stream()
                        .collect(Collectors.toMap(property -> property.getName().toString(),
                                property -> property.getValue().toString()));
        stringMap.put(DittoHeaderDefinition.CORRELATION_ID.getKey(), getCorrelationId(message));
        stringMap.put(DittoHeaderDefinition.CONTENT_TYPE.getKey(), message.getContentType().orElseThrow().toString());
        verifyBlockedHeaders.accept(mapToJsonObject(stringMap));
        return stringMap;
    }

    @Override
    protected BrokerType getBrokerType() {
        return BrokerType.MQTT;
    }

    private BlockingQueue<Mqtt5Publish> subscribe(final String topic) {
        return subscribe(topic, 0);
    }

    BlockingQueue<Mqtt5Publish> subscribe(final String topic, final int qos) {
        final BlockingQueue<Mqtt5Publish> queue = new LinkedBlockingDeque<>();
        logger.debug("mqttClient: subscribe on topic <{}> with qos {}", topic, qos);
        rethrow(() -> {
            mqttClientSupplier.get()
                    .subscribeWith()
                    .topicFilter(topic)
                    .qos(MqttQos.fromCode(qos))
                    .callback(queue::add)
                    .send();
        });
        return queue;
    }

    interface TargetTopicSupplier {

        String getTopic(final String connectionId);
    }

}
