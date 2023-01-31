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
package org.eclipse.ditto.testing.system.connectivity.mqtt3;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
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
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.connectivity.api.ExternalMessage;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonField;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonObjectBuilder;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityWorker;
import org.slf4j.Logger;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;

import akka.util.ByteString;

public final class Mqtt3ConnectivityWorker
        extends AbstractConnectivityWorker<BlockingQueue<Mqtt3Publish>, Mqtt3Publish> {

    private final TargetTopicSupplier topicSupplier;
    private final Supplier<Mqtt3AsyncClient> mqttClientSupplier;
    private final String connectionIdWithEnforcement;
    private final Duration waitTimeout;

    Mqtt3ConnectivityWorker(final Logger logger,
            final TargetTopicSupplier topicSupplier,
            final Supplier<Mqtt3AsyncClient> mqttClientSupplier,
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

    private static void rethrow(final ThrowingRunnable runnable) {
        try {
            runnable.run();
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected BlockingQueue<Mqtt3Publish> initResponseConsumer(
            final String connectionName,
            final String correlationId
    ) {
        return subscribe(correlationId);
    }

    @Override
    protected BlockingQueue<Mqtt3Publish> initTargetsConsumer(final String connectionName) {
        return subscribe(topicSupplier.getTopic(connectionName));
    }

    @Override
    protected BlockingQueue<Mqtt3Publish> initTargetsConsumer(final String connectionName, final String targetAddress) {
        return subscribe(targetAddress);
    }

    @Override
    protected Mqtt3Publish consumeResponse(final String correlationId, final BlockingQueue<Mqtt3Publish> consumer) {
        final AtomicReference<Mqtt3Publish> messageBox = new AtomicReference<>();
        rethrow(() -> messageBox.set(consumer.poll(waitTimeout.toMillis(), TimeUnit.MILLISECONDS)));
        return messageBox.get();
    }

    @Override
    protected CompletableFuture<Mqtt3Publish> consumeResponseInFuture(
            final String correlationId,
            final BlockingQueue<Mqtt3Publish> consumer
    ) {
        return CompletableFuture.supplyAsync(() -> consumeResponse(correlationId, consumer));
    }

    @Override
    protected Mqtt3Publish consumeFromTarget(
            final String connectionName,
            final BlockingQueue<Mqtt3Publish> targetsConsumer
    ) {
        return consumeResponse(connectionName, targetsConsumer);
    }

    @Override
    protected CompletableFuture<Mqtt3Publish> consumeFromTargetInFuture(
            final String connectionName,
            final BlockingQueue<Mqtt3Publish> targetsConsumer
    ) {
        return consumeResponseInFuture(connectionName, targetsConsumer);
    }

    /*
     * WARNING: Does not work with payload mapping because MQTT has no headers!
     */
    @Override
    protected String getCorrelationId(final Mqtt3Publish message) {
        return checkHeaders(message, headers -> {}).get(DittoHeaderDefinition.CORRELATION_ID.getKey());
    }

    @Override
    protected void sendAsJsonString(
            final String connectionName,
            final String correlationId,
            final String stringMessage,
            final Map<String, String> headersMap
    ) {
        final Map<String, String> extraHeaders = new HashMap<>(headersMap);
        extraHeaders.put(ExternalMessage.REPLY_TO_HEADER, correlationId);
        final String thingIdFromHeader = headersMap.getOrDefault("device_id", "invalid:id");
        final String mqttTopic;
        if (connectionIdWithEnforcement.equals(connectionName)) {
            // we need the thingId in the topic for enforcement
            mqttTopic = getPublishTopic(connectionName) + "/" + thingIdFromHeader.replaceFirst(Pattern.quote(":"), "/");
        } else {
            mqttTopic = getPublishTopic(connectionName);
        }
        final JsonObject objectWithHeaders = packHeaders(JsonFactory.newObject(stringMessage), extraHeaders);
        final ByteString bytePayload = ByteString.fromString(objectWithHeaders.toString());
        rethrow(() -> {
            logger.info("mqttClient: publishing on topic <{}>: {}", mqttTopic, bytePayload);
            mqttClientSupplier.get()
                    .publishWith()
                    .topic(mqttTopic)
                    .qos(MqttQos.EXACTLY_ONCE)
                    .payload(bytePayload.toByteBuffer())
                    .send();
        });
    }

    @Override
    protected void sendAsBytePayload(final String connectionName, final String correlationId, final byte[] byteMessage,
            final Map<String, String> headersMap) {

        final String thingIdFromHeader = headersMap.getOrDefault("device_id", "invalid:id");
        final String mqttTopic;
        if (connectionIdWithEnforcement.equals(connectionName)) {
            // we need the thingId in the topic for enforcement
            mqttTopic = getPublishTopic(connectionName) + "/" + thingIdFromHeader.replaceFirst(Pattern.quote(":"), "/");
        } else {
            mqttTopic = getPublishTopic(connectionName);
        }
        rethrow(() -> {
            logger.info("mqttClient: publishing on topic <{}>: {}", mqttTopic, byteMessage);
            mqttClientSupplier.get()
                    .publishWith()
                    .topic(mqttTopic)
                    .qos(MqttQos.EXACTLY_ONCE)
                    .payload(byteMessage)
                    .send();
        });
    }

    @Override
    protected JsonifiableAdaptable jsonifiableAdaptableFrom(final Mqtt3Publish message) {
        return jsonifiableAdaptableFrom(textFrom(message));
    }

    @Override
    protected String textFrom(final Mqtt3Publish message) {
        return new String(message.getPayloadAsBytes());
    }

    @Override
    protected byte[] bytesFrom(final Mqtt3Publish message) {
        return message.getPayloadAsBytes();
    }

    /*
     * WARNING: Does not work with payload mapping because MQTT has no headers!
     */
    @Override
    protected Map<String, String> checkHeaders(final Mqtt3Publish message,
            final Consumer<JsonObject> verifyBlockedHeaders) {
        final JsonObject json = JsonFactory.newObject(textFrom(message));
        final JsonObject headers = json.getValue("headers")
                .filter(JsonValue::isObject)
                .map(JsonValue::asObject)
                .orElseGet(JsonFactory::newObject);
        verifyBlockedHeaders.accept(headers);
        return headers.stream()
                .collect(Collectors.toMap(JsonField::getKeyName, field -> field.getValue().formatAsString()));
    }

    @Override
    protected BrokerType getBrokerType() {
        return BrokerType.MQTT;
    }


    private BlockingQueue<Mqtt3Publish> subscribe(final String topic) {
        return subscribe(topic, 0);
    }

    BlockingQueue<Mqtt3Publish> subscribe(final String topic, final int qos) {
        final BlockingQueue<Mqtt3Publish> queue = new LinkedBlockingDeque<>();
        logger.debug("mqttClient: subscribe on topic <{}> with qos {}", topic, qos);
        rethrow(() -> {
            mqttClientSupplier.get().subscribeWith().topicFilter(topic).callback(queue::add).send();
        });
        return queue;
    }


    interface TargetTopicSupplier {

        String getTopic(final String connectionId);

    }

}
