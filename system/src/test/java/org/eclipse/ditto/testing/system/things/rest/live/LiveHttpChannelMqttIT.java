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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.http.HttpStatus;
import org.assertj.core.api.JUnitSoftAssertions;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.headers.translator.HeaderTranslator;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.connectivity.model.LogCategory;
import org.eclipse.ditto.connectivity.model.LogEntry;
import org.eclipse.ditto.connectivity.model.LogLevel;
import org.eclipse.ditto.connectivity.model.LogType;
import org.eclipse.ditto.json.JsonKey;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.protocol.Payload;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
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
import org.eclipse.ditto.testing.system.things.rest.ThingResource;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttribute;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttributeResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyFeatureResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveAttribute;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveAttributeResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThingResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;

import akka.util.ByteString;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.LogDetail;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;

/**
 * This test uses a MQTT v3 connection and multiple MQTT clients to check the behaviour of live commands via HTTP.
 * The test cases answer the following questions:
 * <ul>
 *     <li>Does live command handling work for MQTT connections?</li>
 *     <li>How is a live command handled if multiple subscribers exist?</li>
 *     <li>How are multiple responses for the same command handled?</li>
 * </ul>
 */
@RunIf(DockerEnvironment.class)
public final class LiveHttpChannelMqttIT {

    private static final String CONNECTION_NAME = LiveHttpChannelMqttIT.class.getSimpleName();
    private static final Duration MQTT_CLIENT_OPERATION_TIMEOUT = Duration.ofSeconds(10L);
    private static final String SIGNAL_TARGET_TOPIC = CONNECTION_NAME + "/target";
    private static final String SIGNAL_SOURCE_TOPIC = CONNECTION_NAME + "/source";
    private static final TestConfig TEST_CONFIG = TestConfig.getInstance();
    private static final MqttConfig MQTT_CONFIG = MqttConfig.of(TEST_CONFIG);

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
            PolicyWithConnectionSubjectResource.newInstance(TEST_SOLUTION_RESOURCE,
                    POLICIES_HTTP_CLIENT_RESOURCE, CONNECTION_NAME);

    @ClassRule(order = 2)
    public static final ConnectionResource MQTT_SOLUTION_CONNECTION_RESOURCE =
            ConnectionResource.newInstance(TEST_CONFIG.getTestEnvironment(),
                    CONNECTIONS_CLIENT_RESOURCE,
                    () -> MqttConnectionFactory.getMqttConnection(MQTT_CONFIG.getTcpUri(),
                            TEST_SOLUTION_RESOURCE.getTestUsername(),
                            CONNECTION_NAME,
                            SIGNAL_TARGET_TOPIC,
                            SIGNAL_SOURCE_TOPIC));

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
    public final Mqtt3AsyncClientResource mqttClientResource1 =
            Mqtt3AsyncClientResource.newInstanceWithRandomClientId(MQTT_CONFIG,
                    MQTT_CLIENT_OPERATION_TIMEOUT);

    @Rule
    public final Mqtt3AsyncClientResource mqttClientResource2 =
            Mqtt3AsyncClientResource.newInstanceWithRandomClientId(MQTT_CONFIG,
                    MQTT_CLIENT_OPERATION_TIMEOUT);

    @Rule
    public final Mqtt3AsyncClientResource mqttClientResource3 =
            Mqtt3AsyncClientResource.newInstanceWithRandomClientId(MQTT_CONFIG,
                    MQTT_CLIENT_OPERATION_TIMEOUT);

    @Rule
    public final Mqtt3AsyncClientResource mqttClientResource4 =
            Mqtt3AsyncClientResource.newInstanceWithRandomClientId(MQTT_CONFIG,
                    MQTT_CLIENT_OPERATION_TIMEOUT);

    @Rule
    public final TestNameCorrelationId testNameCorrelationId = TestNameCorrelationId.newInstance();

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    @BeforeClass
    public static void beforeClass() throws InterruptedException {
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    }

    @Test
    public void sendLiveModifyAttributeViaHttpFansOutToMultipleSubscribers() throws InterruptedException {
        final var signalTransferQueues =
                Stream.of(mqttClientResource1, mqttClientResource2, mqttClientResource3, mqttClientResource4)
                        .collect(Collectors.toMap(Mqtt3AsyncClientResource::getClientId,
                                LiveHttpChannelMqttIT::subscribeForSignalTopicViaTransferQueue));

        RestAssured.given(
                        getBasicThingsRequestSpecWithLiveQuery(testNameCorrelationId.getCorrelationId(".putAttribute")))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "false")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingResource.getThingId().toString(), "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_ACCEPTED);

        for (final var signalTransferQueueEntry : signalTransferQueues.entrySet()) {
            final var signalTransferQueue = signalTransferQueueEntry.getValue();
            final var signalFromTransferQueue = signalTransferQueue.poll(5, TimeUnit.SECONDS);

            softly.assertThat(signalFromTransferQueue)
                    .as("MQTT client <%s> received expected signal.", signalTransferQueueEntry.getKey())
                    .isInstanceOf(ModifyAttribute.class);
        }
    }

    private static TransferQueue<Signal<?>> subscribeForSignalTopicViaTransferQueue(
            final Mqtt3AsyncClientResource mqttClientResource) {

        final var result = new LinkedTransferQueue<Signal<?>>();
        final var mqtt3AsyncClient = mqttClientResource.getClient();
        Mqtt3MessageHelper.subscribeForSignal(mqtt3AsyncClient, SIGNAL_TARGET_TOPIC, result::offer)
                .toCompletableFuture()
                .join();

        return result;
    }

    @Test
    public void sendResponsesFromMultipleSubscribers() {
        final var correlationId = testNameCorrelationId.getCorrelationId(".putAttribute");
        final var attributeKey = JsonKey.of("manufacturer");

        final Predicate<Signal<?>> isExpectedModifyAttributeCommand = signal -> {
            final var signalDittoHeaders = signal.getDittoHeaders();
            final var hasSignalExpectedCorrelationId = signalDittoHeaders.getCorrelationId()
                    .map(CorrelationId::of)
                    .filter(correlationId::equals)
                    .isPresent();

            return ModifyAttribute.TYPE.equals(signal.getType()) && hasSignalExpectedCorrelationId;
        };

        final Consumer<Signal<?>> sendMultipleCommandResponsesSimultaneously = signal -> {
            if (isExpectedModifyAttributeCommand.test(signal)) {
                final var modifyAttributeResponse = ModifyAttributeResponse.modified(thingResource.getThingId(),
                        attributeKey.asPointer(),
                        signal.getDittoHeaders());

                final var respondingTasks = Stream.of(mqttClientResource2, mqttClientResource3, mqttClientResource4)
                        .map(Mqtt3AsyncClientResource::getClient)
                        .map((Function<Mqtt3AsyncClient, Runnable>) mqttClient -> () -> Mqtt3MessageHelper.publishSignal(
                                mqttClient,
                                SIGNAL_SOURCE_TOPIC,
                                modifyAttributeResponse
                        ))
                        .map(Executors::callable)
                        .collect(Collectors.toList());

                simultaneouslyInvokeAll(respondingTasks);
            }
        };

        // Subscribe for HTTP live signals.
        final var mqttClient1 = mqttClientResource1.getClient();
        Mqtt3MessageHelper.subscribeForSignal(
                mqttClient1,
                SIGNAL_TARGET_TOPIC,
                sendMultipleCommandResponsesSimultaneously
        ).toCompletableFuture().join();

        // Send HTTP live request.
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "true")
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), "5s")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}",
                        String.valueOf(thingResource.getThingId()),
                        attributeKey.toString())

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
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
    public void sendInvalidCommandResponseTypeFromDevice() {
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        // Enable connection logs.
        final var connectionsHttpClient = CONNECTIONS_CLIENT_RESOURCE.getConnectionsClient();
        connectionsHttpClient.postConnectionCommand(MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId(),
                "connectivity.commands:enableConnectionLogs",
                baseCorrelationId.withSuffix(".enableConnectionLogs"));

        // Subscribe MQTT client for live signals. Send invalid response.
        final var mqttClient1 = mqttClientResource1.getClient();
        Mqtt3MessageHelper.subscribeForSignal(
                mqttClient1,
                SIGNAL_TARGET_TOPIC,
                signal -> Mqtt3MessageHelper.publishSignal(
                                mqttClient1,
                                SIGNAL_SOURCE_TOPIC,
                                RetrieveThingResponse.of(
                                        thingResource.getThingId(),
                                        thingResource.getThing().toJson(),
                                        signal.getDittoHeaders()
                                )
                        )
                        .toCompletableFuture()
                        .join()
        );

        // Send HTTP live request.
        final var putAttributeValueCorrelationId = baseCorrelationId.withSuffix(".putAttributeValue");
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(putAttributeValueCorrelationId))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "true")
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), "30s")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}",
                        String.valueOf(thingResource.getThingId()),
                        "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_REQUEST_TIMEOUT)
                .body(containsString(String.format(
                        "Received no appropriate live response within the specified timeout." +
                                " An invalid response was received, though: Type of live response <%s> is not related" +
                                " to type of command <%s>.",
                        RetrieveThingResponse.TYPE,
                        ModifyAttribute.TYPE)));

        // Search in connection log for expected failure entry.
        final var connectionLogs = connectionsHttpClient.getConnectionLogs(MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId(),
                baseCorrelationId.withSuffix(".getConnectionLogs"));

        assertThat(LogEntriesFromJsonConverter.getLogEntries(connectionLogs))
                .filteredOn(LogEntry::getCorrelationId, putAttributeValueCorrelationId.toString())
                .filteredOn(LogEntry::getLogLevel, LogLevel.FAILURE)
                .filteredOn(LogEntry::getLogType, LogType.DROPPED)
                .filteredOn(LogEntry::getLogCategory, LogCategory.RESPONSE)
                .filteredOn(LogEntry::getEntityId, Optional.of(thingResource.getThingId()))
                .filteredOnAssertions(logEntry -> assertThat(logEntry.getMessage())
                        .contains(RetrieveThingResponse.TYPE, ModifyAttribute.TYPE))
                .hasSize(1);
    }

    @Test
    public void sendInvalidCommandResponseStatusCodeFromDevice() {
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();
        final var putAttributeValueCorrelationId = baseCorrelationId.withSuffix(".putAttributeValue");
        final var attributePath = "manufacturer";
        final var invalidHttpStatus = org.eclipse.ditto.base.model.common.HttpStatus.IM_A_TEAPOT;

        // Enable connection logs.
        final var connectionsHttpClient = CONNECTIONS_CLIENT_RESOURCE.getConnectionsClient();
        connectionsHttpClient.postConnectionCommand(MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId(),
                "connectivity.commands:enableConnectionLogs",
                baseCorrelationId.withSuffix(".enableConnectionLogs"));

        // Subscribe MQTT client for live signals. Send invalid response.
        final var mqttClient1 = mqttClientResource1.getClient();
        Mqtt3MessageHelper.subscribeForSignal(
                mqttClient1,
                SIGNAL_TARGET_TOPIC,
                signal -> {
                    final var modifyAttribute = (ModifyAttribute) signal;
                    final var modifyAttributeResponse = ModifyAttributeResponse.modified(modifyAttribute.getEntityId(),
                            modifyAttribute.getAttributePointer(),
                            modifyAttribute.getDittoHeaders());
                    final var protocolAdapter = DittoProtocolAdapter.of(HeaderTranslator.empty());
                    final var adaptable = protocolAdapter.toAdaptable(modifyAttributeResponse);
                    final var jsonifiableAdaptable =
                            ProtocolFactory.wrapAsJsonifiableAdaptable(ProtocolFactory.newAdaptableBuilder(adaptable)
                                    .withPayload(Payload.newBuilder(adaptable.getPayload())
                                            .withStatus(invalidHttpStatus)
                                            .build())
                                    .build());
                    final var jsonifiableAdaptableJsonString = jsonifiableAdaptable.toJsonString();
                    final var byteString = ByteString.fromString(jsonifiableAdaptableJsonString);
                    final var byteBuffer = byteString.toByteBuffer();

                    mqttClient1.publishWith()
                            .topic(SIGNAL_SOURCE_TOPIC)
                            .payload(byteBuffer)
                            .send().toCompletableFuture().join();

                }
        );

        // Send HTTP live request.
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(putAttributeValueCorrelationId))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "true")
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), "5s")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", String.valueOf(thingResource.getThingId()), attributePath)

                .then()
                .statusCode(HttpStatus.SC_REQUEST_TIMEOUT);

        // Search in connection log for expected failure entry.
        final var connectionLogs = connectionsHttpClient.getConnectionLogs(MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId(),
                baseCorrelationId.withSuffix(".getConnectionLogs"));

        assertThat(LogEntriesFromJsonConverter.getLogEntries(connectionLogs))
                .filteredOn(LogEntry::getCorrelationId, putAttributeValueCorrelationId.toString())
                .filteredOn(LogEntry::getLogLevel, LogLevel.FAILURE)
                .filteredOn(LogEntry::getLogType, LogType.DROPPED)
                .filteredOn(LogEntry::getLogCategory, LogCategory.RESPONSE)
                .filteredOn(LogEntry::getEntityId, Optional.of(thingResource.getThingId()))
                .filteredOnAssertions(logEntry -> assertThat(logEntry.getMessage())
                        .contains("Live response is invalid:")
                        .contains(invalidHttpStatus + " is invalid. ModifyAttributeResponse only allows"))
                .hasSize(1);
    }

    @Test
    public void sendInvalidCommandResponseStatusCodeFromDeviceRequestingErrorResponse() {
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();
        final var putAttributeValueCorrelationId = baseCorrelationId.withSuffix(".putAttributeValue");
        final var attributePath = "manufacturer";
        final var invalidHttpStatus = org.eclipse.ditto.base.model.common.HttpStatus.IM_A_TEAPOT;

        // Enable connection logs.
        final var connectionsHttpClient = CONNECTIONS_CLIENT_RESOURCE.getConnectionsClient();
        connectionsHttpClient.postConnectionCommand(MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId(),
                "connectivity.commands:enableConnectionLogs",
                baseCorrelationId.withSuffix(".enableConnectionLogs"));

        // Subscribe MQTT client for live signals. Send invalid response.
        final var mqttClient1 = mqttClientResource1.getClient();
        Mqtt3MessageHelper.subscribeForSignal(
                mqttClient1,
                SIGNAL_TARGET_TOPIC,
                signal -> {
                    final var modifyAttribute = (ModifyAttribute) signal;
                    final var modifyAttributeResponse = ModifyAttributeResponse.modified(modifyAttribute.getEntityId(),
                            modifyAttribute.getAttributePointer(),
                            modifyAttribute.getDittoHeaders());
                    final var protocolAdapter = DittoProtocolAdapter.of(HeaderTranslator.empty());
                    final var adaptable = protocolAdapter.toAdaptable(modifyAttributeResponse);
                    final var jsonifiableAdaptable =
                            ProtocolFactory.wrapAsJsonifiableAdaptable(ProtocolFactory.newAdaptableBuilder(adaptable)
                                    .withHeaders(DittoHeaders.newBuilder(modifyAttributeResponse.getDittoHeaders())
                                            .responseRequired(true)
                                            .build())
                                    .withPayload(Payload.newBuilder(adaptable.getPayload())
                                            .withStatus(invalidHttpStatus)
                                            .build())
                                    .build());
                    final var jsonifiableAdaptableJsonString = jsonifiableAdaptable.toJsonString();
                    final var byteString = ByteString.fromString(jsonifiableAdaptableJsonString);
                    final var byteBuffer = byteString.toByteBuffer();

                    mqttClient1.publishWith()
                            .topic(SIGNAL_SOURCE_TOPIC)
                            .payload(byteBuffer)
                            .send().toCompletableFuture().join();

                }
        );

        // Send HTTP live request.
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(putAttributeValueCorrelationId))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "true")
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), "5s")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", String.valueOf(thingResource.getThingId()), attributePath)

                .then()
                .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);

        // Search in connection log for expected failure entry.
        final var connectionLogs = connectionsHttpClient.getConnectionLogs(MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId(),
                baseCorrelationId.withSuffix(".getConnectionLogs"));

        assertThat(LogEntriesFromJsonConverter.getLogEntries(connectionLogs))
                .filteredOn(LogEntry::getCorrelationId, putAttributeValueCorrelationId.toString())
                .filteredOn(LogEntry::getLogLevel, LogLevel.FAILURE)
                .filteredOn(LogEntry::getLogType, LogType.DROPPED)
                .filteredOn(LogEntry::getLogCategory, LogCategory.RESPONSE)
                .filteredOn(LogEntry::getEntityId, Optional.of(thingResource.getThingId()))
                .filteredOnAssertions(logEntry -> assertThat(logEntry.getMessage())
                        .contains("Live response is invalid:")
                        .contains(invalidHttpStatus + " is invalid. ModifyAttributeResponse only allows"))
                .hasSize(1);
    }

    @Test
    public void serializedPutFeatureCommandJsonDoesNotContainHiddenJsonSchemaFields() {
        final var feature = Feature.newBuilder()
                .properties(FeatureProperties.newBuilder()
                        .set("indoor", 22.7)
                        .build())
                .withId("TemperatureSensor")
                .build();

        final var mqttClient = mqttClientResource1.getClient();
        mqttClient.subscribeWith().topicFilter(SIGNAL_TARGET_TOPIC).callback(signal -> {
            final var dittoProtocolMessageJsonString =
                    StandardCharsets.UTF_8.decode(signal.getPayload().orElseThrow()).toString();

            softly.assertThat(dittoProtocolMessageJsonString)
                    .as("no schema version field")
                    .doesNotContain(String.valueOf(JsonSchemaVersion.getJsonKey()));

            final var jsonifiableAdaptable =
                    ProtocolFactory.jsonifiableAdaptableFromJson(JsonObject.of(dittoProtocolMessageJsonString));
            final var modifyFeatureResponse = ModifyFeatureResponse.created(thingResource.getThingId(),
                    feature,
                    jsonifiableAdaptable.getDittoHeaders());

            Mqtt3MessageHelper.publishSignal(mqttClient, SIGNAL_SOURCE_TOPIC, modifyFeatureResponse);
        }).send();

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(testNameCorrelationId.getCorrelationId()))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "true")
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), "5s")
                .body(feature.toJsonString())

                .when()
                .put("/{thingId}/features/{featureId}", thingResource.getThingId().toString(), feature.getId())

                .then()
                .statusCode(HttpStatus.SC_CREATED);
    }

    @Test
    public void sendInvalidQueryCommandResponseFromDeviceThenSendValidCommandResponseFromDevice()
            throws InterruptedException {
        final var attributeName = "manufacturer";
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        // Enable connection logs.
        final var connectionsHttpClient = CONNECTIONS_CLIENT_RESOURCE.getConnectionsClient();
        connectionsHttpClient.postConnectionCommand(MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId(),
                "connectivity.commands:enableConnectionLogs",
                baseCorrelationId.withSuffix(".enableConnectionLogs"));

        // Subscribe MQTT client for live signals. Send invalid response.
        final var mqttClient1 = mqttClientResource1.getClient();
        Mqtt3MessageHelper.subscribeForSignal(
                mqttClient1,
                SIGNAL_TARGET_TOPIC,
                signal -> {
                    final var invalidCommandResponse = RetrieveThingResponse.of(thingResource.getThingId(),
                            thingResource.getThing().toJson(),
                            signal.getDittoHeaders());

                    final var validCommandResponse = RetrieveAttributeResponse.of(thingResource.getThingId(),
                            JsonPointer.of(attributeName),
                            JsonValue.of("Bosch IO"),
                            signal.getDittoHeaders());

                    Mqtt3MessageHelper.publishSignal(mqttClient1, SIGNAL_SOURCE_TOPIC, invalidCommandResponse)
                            .whenComplete((result, error) -> {
                                try {
                                    TimeUnit.MILLISECONDS.sleep(200);
                                } catch (final InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                Mqtt3MessageHelper.publishSignal(
                                        mqttClient1,
                                        SIGNAL_SOURCE_TOPIC,
                                        validCommandResponse
                                );
                            })
                            .toCompletableFuture()
                            .join();
                }
        );

        // Send HTTP live request.
        final var getAttribute = baseCorrelationId.withSuffix(".getAttribute");
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(getAttribute))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "true")
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), "5s")

                .when()
                .get("/{thingId}/attributes/{attributePath}", String.valueOf(thingResource.getThingId()), attributeName)

                .then()
                .statusCode(HttpStatus.SC_OK);

        // Search in connection log for expected failure entry.
        final var connectionLogs = connectionsHttpClient.getConnectionLogs(MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId(),
                baseCorrelationId.withSuffix(".getConnectionLogs"));

        assertThat(LogEntriesFromJsonConverter.getLogEntries(connectionLogs))
                .filteredOn(LogEntry::getCorrelationId, getAttribute.toString())
                .filteredOn(LogEntry::getLogLevel, LogLevel.FAILURE)
                .filteredOn(LogEntry::getLogType, LogType.DROPPED)
                .filteredOn(LogEntry::getLogCategory, LogCategory.RESPONSE)
                .filteredOn(LogEntry::getEntityId, Optional.of(thingResource.getThingId()))
                .filteredOnAssertions(logEntry -> assertThat(logEntry.getMessage())
                        .contains(RetrieveThingResponse.TYPE, RetrieveAttribute.TYPE))
                .hasSize(1);
    }

    @Test
    public void sendInvalidModifyCommandResponseFromDeviceThenSendValidCommandResponseFromDevice() {
        final var attributeName = "manufacturer";
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        // Enable connection logs.
        final var connectionsHttpClient = CONNECTIONS_CLIENT_RESOURCE.getConnectionsClient();
        connectionsHttpClient.postConnectionCommand(MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId(),
                "connectivity.commands:enableConnectionLogs",
                baseCorrelationId.withSuffix(".enableConnectionLogs"));

        // Subscribe MQTT client for live signals. Send invalid response.
        final var mqttClient1 = mqttClientResource1.getClient();
        Mqtt3MessageHelper.subscribeForSignal(
                mqttClient1,
                SIGNAL_TARGET_TOPIC,
                signal -> {
                    final var invalidCommandResponse = RetrieveThingResponse.of(thingResource.getThingId(),
                            thingResource.getThing().toJson(),
                            signal.getDittoHeaders());

                    final var validCommandResponse = ModifyAttributeResponse.modified(thingResource.getThingId(),
                            JsonPointer.of(attributeName),
                            signal.getDittoHeaders());

                    Mqtt3MessageHelper.publishSignal(mqttClient1, SIGNAL_SOURCE_TOPIC, invalidCommandResponse)
                            .whenComplete((result, error) -> {
                                try {
                                    TimeUnit.MILLISECONDS.sleep(200);
                                } catch (final InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                Mqtt3MessageHelper.publishSignal(
                                        mqttClient1,
                                        SIGNAL_SOURCE_TOPIC,
                                        validCommandResponse
                                );
                            })
                            .toCompletableFuture()
                            .join();
                }
        );

        // Send HTTP live request.
        final var putAttributeValueCorrelationId = baseCorrelationId.withSuffix(".putAttributeValue");
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(putAttributeValueCorrelationId))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "true")
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), "5s")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", String.valueOf(thingResource.getThingId()), attributeName)

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        // Search in connection log for expected failure entry.
        final var connectionLogs = connectionsHttpClient.getConnectionLogs(MQTT_SOLUTION_CONNECTION_RESOURCE.getConnectionId(),
                baseCorrelationId.withSuffix(".getConnectionLogs"));

        assertThat(LogEntriesFromJsonConverter.getLogEntries(connectionLogs))
                .filteredOn(LogEntry::getCorrelationId, putAttributeValueCorrelationId.toString())
                .filteredOn(LogEntry::getLogLevel, LogLevel.FAILURE)
                .filteredOn(LogEntry::getLogType, LogType.DROPPED)
                .filteredOn(LogEntry::getLogCategory, LogCategory.RESPONSE)
                .filteredOn(LogEntry::getEntityId, Optional.of(thingResource.getThingId()))
                .filteredOnAssertions(logEntry -> assertThat(logEntry.getMessage())
                        .contains(RetrieveThingResponse.TYPE, ModifyAttribute.TYPE))
                .hasSize(1);
    }

    private static RequestSpecification getBasicThingsRequestSpecWithLiveQuery(final CorrelationId correlationId) {
        return new RequestSpecBuilder()
                .setBaseUri(THINGS_BASE_URI_RESOURCE.getThingsBaseUriApi2())
                .addQueryParam(DittoHeaderDefinition.CHANNEL.getKey(), "live")
                .setAuth(RestAssured.oauth2(TEST_SOLUTION_RESOURCE.getAccessToken().getToken()))
                .setContentType(ContentType.JSON)
                .addHeader(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId.toString())
                .log(LogDetail.ALL)
                .build();
    }

}
