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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.eclipse.ditto.json.JsonCollectors;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonKey;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonParseException;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.junit.Assert;
import org.slf4j.Logger;

/**
 * Abstract class for building connectivity in connectivity tests.
 *
 * @param <C> the consumer
 * @param <M> the consumer message
 */
public abstract class AbstractConnectivityWorker<C, M> {

    protected final Logger logger;

    protected AbstractConnectivityWorker(final Logger logger) {
        this.logger = logger;
    }

    protected static RuntimeException mapException(final Exception e) {
        if (e instanceof RuntimeException) {
            return (RuntimeException) e;
        } else {
            return new IllegalStateException(e);
        }
    }

    protected abstract C initResponseConsumer(String connectionName, String correlationId);

    protected abstract C initTargetsConsumer(String connectionName);

    protected abstract C initTargetsConsumer(final String connectionName, final String targetAddress);

    protected abstract M consumeResponse(String correlationId, C consumer);

    protected abstract CompletableFuture<M> consumeResponseInFuture(String correlationId, C consumer);

    protected abstract M consumeFromTarget(String connectionName, C targetsConsumer);

    protected abstract CompletableFuture<M> consumeFromTargetInFuture(String connectionName, C targetsConsumer);

    protected abstract String getCorrelationId(M message);

    protected abstract void sendAsJsonString(String connectionName, String correlationId, String stringMessage,
            Map<String, String> headersMap);

    protected abstract void sendAsBytePayload(String connectionName, String correlationId, byte[] byteMessage,
            Map<String, String> headersMap);

    protected abstract JsonifiableAdaptable jsonifiableAdaptableFrom(M message);

    protected JsonifiableAdaptable jsonifiableAdaptableFrom(final String string) {
        final JsonObject bodyJson;
        try {
            bodyJson = Optional.of(JsonFactory.readFrom(string))
                    .filter(JsonValue::isObject)
                    .map(JsonValue::asObject)
                    .orElseThrow(() -> new JsonParseException(
                            "The response message was not a JSON object as required:" + string));
        } catch (final JsonParseException e) {
            Assert.fail("Got unknown non-JSON response message: " + string);
            throw e;
        }
        return ProtocolFactory.jsonifiableAdaptableFromJson(bodyJson);
    }

    protected abstract String textFrom(M message);

    protected abstract byte[] bytesFrom(M message);

    protected abstract Map<String, String> checkHeaders(M message, final Consumer<JsonObject> verifyBlockedHeaders);

    protected JsonObject mapToJsonObject(final Map<String, String> map) {
        return map.entrySet()
                .stream()
                .map(entry -> JsonFactory.newField(JsonKey.of(entry.getKey()), JsonFactory.newValue(entry.getValue())))
                .collect(JsonCollectors.fieldsToObject());
    }

    protected abstract BrokerType getBrokerType();

    public enum BrokerType {
        RABBITMQ("rabbitmq"),
        AMQP10("amqp10"),
        MQTT("mqtt"),
        KAFKA("kafka"),
        HTTP_PUSH("http-push");

        private final String name;

        BrokerType(final String name) {

            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

}
