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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;

import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt3.message.subscribe.suback.Mqtt3SubAck;

public final class Mqtt3MessageHelper {

    private Mqtt3MessageHelper() {
        throw new AssertionError();
    }

    public static CompletableFuture<Mqtt3SubAck> subscribeForSignal(
            final Mqtt3AsyncClient mqtt3AsyncClient,
            final String targetTopic,
            final Consumer<Signal<?>> signalConsumer
    ) {
        return mqtt3AsyncClient.subscribeWith()
                .topicFilter(targetTopic)
                .callback(message -> signalConsumer.accept(getSignalForMqtt3Publish(message)))
                .send();
    }

    private static Signal<?> getSignalForMqtt3Publish(final Mqtt3Publish mqtt3Publish) {
        return getSignal(getJsonifiableAdaptable(JsonObject.of(getPayloadOrThrow(mqtt3Publish))));
    }

    private static byte[] getPayloadOrThrow(final Mqtt3Publish mqtt3Publish) {
        final var result = mqtt3Publish.getPayloadAsBytes();
        if (0 == result.length) {
            throw new IllegalStateException("Mqtt3Publish has no payload.");
        }
        return result;
    }

    private static JsonifiableAdaptable getJsonifiableAdaptable(final JsonObject jsonObject) {
        return ProtocolFactory.jsonifiableAdaptableFromJson(jsonObject);
    }

    private static Signal<?> getSignal(final JsonifiableAdaptable jsonifiableAdaptable) {
        final var dittoProtocolAdapter = DittoProtocolAdapter.newInstance();
        return dittoProtocolAdapter.fromAdaptable(jsonifiableAdaptable);
    }

    public static CompletableFuture<Mqtt3Publish> publishSignal(
            final Mqtt3AsyncClient mqtt3AsyncClient,
            final String sourceTopic,
            final Signal<?> signal
    ) {
        return mqtt3AsyncClient.publishWith()
                .topic(sourceTopic)
                .payload(getDittoProtocolMessageJsonBytes(signal))
                .send();
    }

    private static ByteBuffer getDittoProtocolMessageJsonBytes(final Signal<?> signal) {
        return ByteBuffer.wrap(getUtf8Bytes(getJsonString(getJsonifiableAdaptable(getAdaptable(signal)))));
    }

    private static Adaptable getAdaptable(final Signal<?> signal) {
        final var dittoProtocolAdapter = DittoProtocolAdapter.newInstance();
        return dittoProtocolAdapter.toAdaptable(signal);
    }

    private static JsonifiableAdaptable getJsonifiableAdaptable(final Adaptable adaptable) {
        return ProtocolFactory.wrapAsJsonifiableAdaptable(adaptable);
    }

    private static String getJsonString(final JsonifiableAdaptable jsonifiableAdaptable) {
        return jsonifiableAdaptable.toJsonString();
    }

    private static byte[] getUtf8Bytes(final String string) {
        return string.getBytes(StandardCharsets.UTF_8);
    }

}
