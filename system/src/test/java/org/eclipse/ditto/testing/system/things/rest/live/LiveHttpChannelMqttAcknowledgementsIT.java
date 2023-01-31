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
package org.eclipse.ditto.testing.system.things.rest.live;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgement;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.ConnectionsHttpClientResource;
import org.eclipse.ditto.testing.common.TestSolutionResource;
import org.eclipse.ditto.testing.common.ThingJsonProducer;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.config.ThingsBaseUriResource;
import org.eclipse.ditto.testing.common.connectivity.ConnectionResource;
import org.eclipse.ditto.testing.common.connectivity.mqtt.Mqtt3AsyncClientResource;
import org.eclipse.ditto.testing.common.connectivity.mqtt.Mqtt3MessageHelper;
import org.eclipse.ditto.testing.common.connectivity.mqtt.MqttConfig;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.correlationid.TestNameCorrelationId;
import org.eclipse.ditto.testing.common.policies.PoliciesHttpClientResource;
import org.eclipse.ditto.testing.common.policies.PolicyWithConnectionSubjectResource;
import org.eclipse.ditto.testing.common.things.ThingsHttpClientResource;
import org.eclipse.ditto.testing.common.ws.ThingsWebSocketClientFactory;
import org.eclipse.ditto.testing.common.ws.ThingsWebsocketClient;
import org.eclipse.ditto.testing.system.things.rest.ThingResource;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttributeResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveAttributeResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;

/**
 * This system test checks the handling of acknowledgements in conjunction with HTTP live requests.
 */
@RunIf(DockerEnvironment.class)
public final class LiveHttpChannelMqttAcknowledgementsIT {

    private static final String CONNECTION_NAME = LiveHttpChannelMqttAcknowledgementsIT.class.getSimpleName();
    private static final Duration MQTT_CLIENT_OPERATION_TIMEOUT = Duration.ofSeconds(10L);
    private static final String SIGNAL_TARGET_TOPIC = CONNECTION_NAME + "/target";
    private static final String SIGNAL_SOURCE_TOPIC = CONNECTION_NAME + "/source";
    private static final TestConfig TEST_CONFIG = TestConfig.getInstance();
    private static final MqttConfig MQTT_CONFIG = MqttConfig.of(TEST_CONFIG);
    private static final String ATTRIBUTE_KEY = "manufacturer";
    private static final JsonPointer ATTRIBUTE_JSON_POINTER = JsonPointer.of(ATTRIBUTE_KEY);
    private static final JsonValue ATTRIBUTE_JSON_VALUE = JsonValue.of("Bosch IO");
    private static final String ATTRIBUTE_VALUE = String.valueOf(ATTRIBUTE_JSON_VALUE);

    @ClassRule(order = 0)
    public static final TestSolutionResource TEST_SOLUTION_RESOURCE = TestSolutionResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 1)
    public static final ConnectionsHttpClientResource CONNECTIONS_CLIENT_RESOURCE =
            ConnectionsHttpClientResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 1)
    public static final PoliciesHttpClientResource POLICIES_HTTP_CLIENT_RESOURCE =
            PoliciesHttpClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @ClassRule(order = 1)
    public static final ThingsHttpClientResource THINGS_HTTP_CLIENT_RESOURCE =
            ThingsHttpClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @ClassRule(order = 2)
    public static final PolicyWithConnectionSubjectResource POLICY_WITH_CONNECTION_SUBJECT_RESOURCE =
            PolicyWithConnectionSubjectResource.newInstance(POLICIES_HTTP_CLIENT_RESOURCE,
                    CONNECTION_NAME);

    @ClassRule(order = 2)
    public static final ConnectionResource MQTT_SOLUTION_CONNECTION_RESOURCE =
            ConnectionResource.newInstance(TEST_CONFIG.getTestEnvironment(),
                    CONNECTIONS_CLIENT_RESOURCE, () -> MqttConnectionFactory.getMqttConnection(MQTT_CONFIG.getTcpUri(),
                            CONNECTION_NAME,
                            SIGNAL_TARGET_TOPIC,
                            SIGNAL_SOURCE_TOPIC,
                            AcknowledgementLabel.of("{{connection:id}}:foo"),
                            AcknowledgementLabel.of("{{connection:id}}:bar"),
                            AcknowledgementLabel.of("{{connection:id}}:baz"),
                            AcknowledgementLabel.of("{{connection:id}}:qux")));

    @ClassRule
    public static final ThingsBaseUriResource THINGS_BASE_URI_RESOURCE = ThingsBaseUriResource.newInstance(TEST_CONFIG);

    @Rule
    public final ThingResource thingResource = ThingResource.forThingSupplier(THINGS_HTTP_CLIENT_RESOURCE, () -> {
        final var thingJsonProducer = new ThingJsonProducer();
        return ThingsModelFactory.newThingBuilder(thingJsonProducer.getThing())
                .setPolicyId(POLICY_WITH_CONNECTION_SUBJECT_RESOURCE.getPolicyId())
                .build();
    });

    @Rule
    public final TestNameCorrelationId testNameCorrelationId = TestNameCorrelationId.newInstance();

    @Rule
    public final Mqtt3AsyncClientResource mqttClientResource1 =
            Mqtt3AsyncClientResource.newInstanceWithRandomClientId(MQTT_CONFIG, MQTT_CLIENT_OPERATION_TIMEOUT);

    @Rule
    public final Mqtt3AsyncClientResource mqttClientResource2 =
            Mqtt3AsyncClientResource.newInstanceWithRandomClientId(MQTT_CONFIG, MQTT_CLIENT_OPERATION_TIMEOUT);

    @Rule
    public final Mqtt3AsyncClientResource mqttClientResource3 =
            Mqtt3AsyncClientResource.newInstanceWithRandomClientId(MQTT_CONFIG, MQTT_CLIENT_OPERATION_TIMEOUT);

    @Rule
    public final Mqtt3AsyncClientResource mqttClientResource4 =
            Mqtt3AsyncClientResource.newInstanceWithRandomClientId(MQTT_CONFIG, MQTT_CLIENT_OPERATION_TIMEOUT);

    @BeforeClass
    public static void beforeClass() {
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    }

    @Test
    public void sendLiveGetAttributeWithCustomAckRequestWithAcknowledgement() {
        final var mqttConnectionId = MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId();
        final var acknowledgementLabel = AcknowledgementLabel.of(mqttConnectionId + ":foo");

        final var mqttClient1 = mqttClientResource1.getClient();
        Mqtt3MessageHelper.subscribeForSignal(
                mqttClient1,
                SIGNAL_TARGET_TOPIC,
                signal -> {
                    final var dittoHeaders = signal.getDittoHeaders();
                    Mqtt3MessageHelper.publishSignal(
                            mqttClient1,
                            SIGNAL_SOURCE_TOPIC,
                            RetrieveAttributeResponse.of(
                                    thingResource.getThingId(),
                                    ATTRIBUTE_JSON_POINTER,
                                    ATTRIBUTE_JSON_VALUE,
                                    dittoHeaders
                            )
                    );
                    Mqtt3MessageHelper.publishSignal(
                            mqttClient1,
                            SIGNAL_SOURCE_TOPIC,
                            getAcknowledgement(acknowledgementLabel, dittoHeaders)
                    );
                }
        );

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(testNameCorrelationId.getCorrelationId()))
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), "5s")
                .header(DittoHeaderDefinition.REQUESTED_ACKS.getKey(), acknowledgementLabel.toString())

                .when()
                .get("/{thingId}/attributes/{attributePath}", thingResource.getThingId().toString(), ATTRIBUTE_KEY)

                .then()
                .statusCode(HttpStatus.OK.getCode());
    }

    @Test
    public void sendLiveModifyAttributeWithCustomAckRequestWithoutAcknowledgement() {
        final var mqttConnectionId = MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId();
        final var acknowledgementLabel = AcknowledgementLabel.of(mqttConnectionId + ":foo");

        final var mqttClient1 = mqttClientResource1.getClient();
        Mqtt3MessageHelper.subscribeForSignal(
                mqttClient1,
                SIGNAL_TARGET_TOPIC,
                signal -> Mqtt3MessageHelper.publishSignal(
                        mqttClient1,
                        SIGNAL_SOURCE_TOPIC,
                        getModifyAttributeResponse(signal.getDittoHeaders())
                )
        );

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(testNameCorrelationId.getCorrelationId()))
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), "30s")
                .header(DittoHeaderDefinition.REQUESTED_ACKS.getKey(), acknowledgementLabel.toString())
                .body(ATTRIBUTE_VALUE)

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingResource.getThingId().toString(), ATTRIBUTE_KEY)

                .then()
                .statusCode(HttpStatus.FAILED_DEPENDENCY.getCode());
    }

    @Test
    public void sendLiveModifyAttributeWithCustomAckRequestWithSingleAcknowledgement() {
        final var mqttConnectionId = MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId();
        final var acknowledgementLabel = AcknowledgementLabel.of(mqttConnectionId + ":foo");

        final var mqttClient1 = mqttClientResource1.getClient();
        Mqtt3MessageHelper.subscribeForSignal(
                mqttClient1,
                SIGNAL_TARGET_TOPIC,
                signal -> {
                    final var dittoHeaders = signal.getDittoHeaders();
                    Mqtt3MessageHelper.publishSignal(mqttClient1,
                            SIGNAL_SOURCE_TOPIC,
                            getAcknowledgement(acknowledgementLabel, dittoHeaders));
                    Mqtt3MessageHelper.publishSignal(mqttClient1,
                            SIGNAL_SOURCE_TOPIC,
                            getModifyAttributeResponse(dittoHeaders));
                }
        );

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(testNameCorrelationId.getCorrelationId()))
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), "5s")
                .header(DittoHeaderDefinition.REQUESTED_ACKS.getKey(), acknowledgementLabel.toString())
                .body(ATTRIBUTE_VALUE)

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingResource.getThingId().toString(), ATTRIBUTE_KEY)

                .then()
                .statusCode(HttpStatus.OK.getCode())
                .log().everything();
    }

    @Test
    public void sendLiveModifyAttributeWithCustomAckRequestWithSameAcknowledgementMultipleTimes() {
        final var mqttConnectionId = MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId();
        final var ackLabel = AcknowledgementLabel.of(mqttConnectionId + ":foo");
        final var mqttClient1 = mqttClientResource1.getClient();

        final Consumer<Signal<?>> publishMultipleAcknowledgementsSimultaneously = signal -> {
            Mqtt3MessageHelper.publishSignal(mqttClient1,
                    SIGNAL_SOURCE_TOPIC,
                    getModifyAttributeResponse(signal.getDittoHeaders()));

            final var publishAcknowledgmentTasks =
                    Stream.of(mqttClientResource1, mqttClientResource2, mqttClientResource3, mqttClientResource4)
                            .map(Mqtt3AsyncClientResource::getClient)
                            .map((Function<Mqtt3AsyncClient, Runnable>) mqttClient -> () -> Mqtt3MessageHelper.publishSignal(
                                    mqttClient,
                                    SIGNAL_SOURCE_TOPIC,
                                    getAcknowledgement(ackLabel, signal.getDittoHeaders())
                            ))
                            .map(Executors::callable)
                            .collect(Collectors.toList());

            simultaneouslyInvokeAll(publishAcknowledgmentTasks);
        };

        // Subscribe for HTTP live signals.
        Mqtt3MessageHelper.subscribeForSignal(
                mqttClient1,
                SIGNAL_TARGET_TOPIC,
                publishMultipleAcknowledgementsSimultaneously
        );

        // Send HTTP live request.
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(testNameCorrelationId.getCorrelationId()))
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), "5s")
                .header(DittoHeaderDefinition.REQUESTED_ACKS.getKey(), ackLabel.toString())
                .body(ATTRIBUTE_VALUE)

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingResource.getThingId().toString(), ATTRIBUTE_KEY)

                .then()
                .statusCode(HttpStatus.OK.getCode());
    }

    private static <T> void simultaneouslyInvokeAll(final Collection<Callable<T>> tasks) {
        final var executorService = Executors.newFixedThreadPool(tasks.size());
        try {
            executorService.invokeAll(tasks);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            executorService.shutdown();
        }
    }

    @Test
    public void sendLiveModifyAttributeWithCustomAckRequestsAnsweredFromDifferentSubscribers() {
        final var mqttConnectionId = MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId();
        final var ackLabelsPerMqttClient = Map.of(
                mqttClientResource1.getClient(), AcknowledgementLabel.of(mqttConnectionId + ":foo"),
                mqttClientResource2.getClient(), AcknowledgementLabel.of(mqttConnectionId + ":bar"),
                mqttClientResource3.getClient(), AcknowledgementLabel.of(mqttConnectionId + ":baz"),
                mqttClientResource4.getClient(), AcknowledgementLabel.of(mqttConnectionId + ":qux")
        );

        final var mqttClient1 = mqttClientResource1.getClient();
        Mqtt3MessageHelper.subscribeForSignal(
                mqttClient1,
                SIGNAL_TARGET_TOPIC,
                signal -> {
                    Mqtt3MessageHelper.publishSignal(mqttClient1,
                            SIGNAL_SOURCE_TOPIC,
                            getModifyAttributeResponse(signal.getDittoHeaders())
                    );

                    final var publishAcknowledgementTasks = ackLabelsPerMqttClient.entrySet()
                            .stream()
                            .map(entry -> {
                                final var mqttClient = entry.getKey();
                                return (Runnable) () -> Mqtt3MessageHelper.publishSignal(mqttClient,
                                        SIGNAL_SOURCE_TOPIC,
                                        getAcknowledgement(entry.getValue(), signal.getDittoHeaders()));
                            })
                            .map(Executors::callable)
                            .collect(Collectors.toList());

                    simultaneouslyInvokeAll(publishAcknowledgementTasks);
                }
        );

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(testNameCorrelationId.getCorrelationId()))
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), "5s")
                .header(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                        String.valueOf(JsonArray.of(ackLabelsPerMqttClient.values())))
                .body(ATTRIBUTE_VALUE)

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingResource.getThingId().toString(), ATTRIBUTE_KEY)

                .then()
                .statusCode(HttpStatus.OK.getCode());
    }

    @Test
    public void sendLiveModifyAttributeWithCustomAckRequestWithSingleAcknowledgementFromDifferentConnection() {
        final var correlationId = testNameCorrelationId.getCorrelationId();
        final var mqttConnectionId = MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId();
        final var ackLabel = AcknowledgementLabel.of(mqttConnectionId + ":foo");

        final var thingsWebSocketClient = getWebSocketClient();
        thingsWebSocketClient.onSignal(signal -> {
            try {
                thingsWebSocketClient.emit(getModifyAttributeResponse(signal.getDittoHeaders()));
            } finally {
                thingsWebSocketClient.close();
            }
        });

        final var mqttClient1 = mqttClientResource1.getClient();
        Mqtt3MessageHelper.subscribeForSignal(
                mqttClient1,
                SIGNAL_TARGET_TOPIC,
                signal -> Mqtt3MessageHelper.publishSignal(
                        mqttClient1,
                        SIGNAL_SOURCE_TOPIC,
                        getAcknowledgement(ackLabel, signal.getDittoHeaders())
                )
        );

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), "5s")
                .header(DittoHeaderDefinition.REQUESTED_ACKS.getKey(), ackLabel.toString())
                .body(ATTRIBUTE_VALUE)

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingResource.getThingId().toString(), ATTRIBUTE_KEY)

                .then()
                .statusCode(HttpStatus.OK.getCode());
    }

    private ThingsWebsocketClient getWebSocketClient() {
        final var webSocketAckLabel = AcknowledgementLabel.of(TEST_SOLUTION_RESOURCE.getTestUsername() + ":foo");
        final var additionalHeaders = DittoHeaders.newBuilder()
                .putHeader(DittoHeaderDefinition.DECLARED_ACKS.getKey(), JsonArray.of(webSocketAckLabel).toString())
                .build();
        final var result =
                ThingsWebSocketClientFactory.getWebSocketClient(TEST_CONFIG, TEST_SOLUTION_RESOURCE, additionalHeaders);
        result.connect(testNameCorrelationId.getCorrelationId(".connectWebSocketClient"));
        result.sendProtocolCommand("START-SEND-LIVE-COMMANDS", "START-SEND-LIVE-COMMANDS:ACK").join();
        return result;
    }

    private ModifyAttributeResponse getModifyAttributeResponse(final DittoHeaders dittoHeaders) {
        return ModifyAttributeResponse.modified(thingResource.getThingId(), ATTRIBUTE_JSON_POINTER, dittoHeaders);
    }

    private Acknowledgement getAcknowledgement(final AcknowledgementLabel acknowledgementLabel,
            final DittoHeaders dittoHeaders) {

        return Acknowledgement.of(acknowledgementLabel, thingResource.getThingId(), HttpStatus.OK, dittoHeaders);
    }

    private static RequestSpecification getBasicThingsRequestSpecWithLiveQuery(final CorrelationId correlationId) {
        return new RequestSpecBuilder()
                .setBaseUri(THINGS_BASE_URI_RESOURCE.getThingsBaseUriApi2())
                .addQueryParam(DittoHeaderDefinition.CHANNEL.getKey(), "live")
                .setAuth(RestAssured.oauth2(TEST_SOLUTION_RESOURCE.getAccessToken().getToken()))
                .setContentType(ContentType.JSON)
                .addHeader(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId.toString())
                .build();
    }

}
