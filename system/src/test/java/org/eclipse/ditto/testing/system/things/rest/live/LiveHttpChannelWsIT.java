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

import static org.hamcrest.Matchers.containsString;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.http.HttpStatus;
import org.assertj.core.api.JUnitSoftAssertions;
import org.eclipse.ditto.base.model.exceptions.TooManyRequestsException;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.WithDittoHeaders;
import org.eclipse.ditto.base.model.headers.translator.HeaderTranslator;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.internal.utils.akka.logging.DittoLogger;
import org.eclipse.ditto.internal.utils.akka.logging.DittoLoggerFactory;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.protocol.Payload;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
import org.eclipse.ditto.protocol.mappingstrategies.IllegalAdaptableException;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.TestSolutionResource;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.config.ThingsBaseUriResource;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.correlationid.TestNameCorrelationId;
import org.eclipse.ditto.testing.common.matcher.BodyContainsOnlyExpectedJsonValueMatcher;
import org.eclipse.ditto.testing.common.things.ThingsHttpClientResource;
import org.eclipse.ditto.testing.common.ws.ThingsWebSocketClientResource;
import org.eclipse.ditto.testing.common.ws.ThingsWebsocketClient;
import org.eclipse.ditto.testing.system.things.rest.ThingResource;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.ThingErrorResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.DeleteThing;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttribute;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttributeResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveAttribute;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveAttributeResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveAttributes;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveAttributesResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThingResponse;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.LogDetail;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;

/**
 * Websocket answered system-tests for live http channel.
 * At the moment only 'amqp' is tested. Other connectivity implementations need a different setup and additional
 * {@link org.eclipse.ditto.testing.common.client.ditto_protocol.DittoProtocolClient} implementations.
 * The testcases themselves don't care about the connectivity channel.
 */
public final class LiveHttpChannelWsIT {

    private static final DittoLogger LOGGER = DittoLoggerFactory.getLogger(LiveHttpChannelWsIT.class);

    private static final int TEST_TIMEOUT = 180;
    private static final TimeUnit TEST_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private static final int DEVICE_TIMEOUT = 30;
    private static final TimeUnit DEVICE_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private static final String DEVICE_TIMEOUT_STRING = DEVICE_TIMEOUT + "s";

    private static final TestConfig TEST_CONFIG = TestConfig.getInstance();

    @ClassRule(order = 0)
    public static final TestSolutionResource TEST_SOLUTION_RESOURCE = TestSolutionResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 1)
    public static final ThingsHttpClientResource THINGS_HTTP_CLIENT_RESOURCE =
            ThingsHttpClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @ClassRule(order = 2)
    public static final ThingResource THING_RESOURCE = ThingResource.fromThingJsonProducer(THINGS_HTTP_CLIENT_RESOURCE);

    @ClassRule
    public static final ThingsBaseUriResource THINGS_BASE_URI_RESOURCE = ThingsBaseUriResource.newInstance(TEST_CONFIG);

    private static ThingId thingId;

    @Rule(order = 1)
    public final ThingsWebSocketClientResource webSocketClientResource =
            ThingsWebSocketClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @Rule
    public final TestNameCorrelationId testNameCorrelationId = TestNameCorrelationId.newInstance();

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    @Rule
    public final Timeout timeout = new Timeout(TEST_TIMEOUT, TEST_TIMEOUT_UNIT);

    private ThingsWebsocketClient thingsWebSocketClient;

    @BeforeClass
    public static void beforeClass() {
        thingId = THING_RESOURCE.getThingId();
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    }

    @Before
    public void before() {
        thingsWebSocketClient = webSocketClientResource.getThingsWebsocketClient();
        thingsWebSocketClient.sendProtocolCommand("START-SEND-LIVE-COMMANDS", "START-SEND-LIVE-COMMANDS:ACK").join();
    }

    @After
    public void after() {
        thingsWebSocketClient.sendProtocolCommand("STOP-SEND-LIVE-COMMANDS", "STOP-SEND-LIVE-COMMANDS:ACK").join();
    }

    @Test
    public void sendLiveModifyAttributeWithoutResponse() throws InterruptedException {
        final var correlationId = testNameCorrelationId.getCorrelationId();

        final var queue = new LinkedTransferQueue<Signal<?>>();
        thingsWebSocketClient.onSignal(queue::offer);

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "false")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_ACCEPTED);

        final var signal = queue.take();

        softly.assertThat(signal).isInstanceOf(ModifyAttribute.class);
        assertIsLiveWithCorrelationId(signal, correlationId);
    }

    @Test
    public void sendLiveModifyAttribute() throws InterruptedException {
        final var correlationId = testNameCorrelationId.getCorrelationId();
        final var handledByDevice = new CountDownLatch(1);

        thingsWebSocketClient.onSignal(signal -> {
            final var receivedDittoHeaders = signal.getDittoHeaders();

            softly.assertThat(signal).isInstanceOf(ModifyAttribute.class);
            softly.assertThat(receivedDittoHeaders.isResponseRequired()).isTrue();
            assertIsLiveWithCorrelationId(signal, correlationId);

            final var modifyAttribute = (ModifyAttribute) signal;
            final var response = ModifyAttributeResponse.modified(
                    modifyAttribute.getEntityId(),
                    modifyAttribute.getAttributePointer(),
                    receivedDittoHeaders);

            thingsWebSocketClient.emit(response);
            handledByDevice.countDown();
        });

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        assertDeviceHandledMessage(handledByDevice);
    }

    @Test
    public void sendLiveRetrieveAttributes() throws InterruptedException {
        final var correlationId = testNameCorrelationId.getCorrelationId();
        final var handledByDevice = new CountDownLatch(1);

        final var attributes = Attributes.newBuilder()
                .set("status", "active")
                .set("failure", false)
                .set("secretAttribute", "verySecret")
                .build();

        thingsWebSocketClient.onSignal(signal -> {
            softly.assertThat(signal).isInstanceOf(RetrieveAttributes.class);
            assertIsLiveWithCorrelationId(signal, correlationId);

            final var retrieveAttributesAttributes = (RetrieveAttributes) signal;
            final var response = RetrieveAttributesResponse.of(retrieveAttributesAttributes.getEntityId(),
                    attributes,
                    signal.getDittoHeaders());

            thingsWebSocketClient.emit(response);
            handledByDevice.countDown();
        });

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")

                .when()
                .get("/{thingId}/attributes", thingId.toString())

                .then()
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(attributes.toJson()))
                .statusCode(HttpStatus.SC_OK);

        assertDeviceHandledMessage(handledByDevice);
    }

    @Test
    public void verifyTimeoutParameter() throws InterruptedException {
        final var correlationId = testNameCorrelationId.getCorrelationId();
        final var handledByDevice = new CountDownLatch(1);

        thingsWebSocketClient.onSignal(signal -> {
            softly.assertThat(signal).isInstanceOf(RetrieveAttributes.class);
            assertIsLiveWithCorrelationId(signal, correlationId);
            handledByDevice.countDown();
        });

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(HttpHeader.TIMEOUT.getName(), DEVICE_TIMEOUT_STRING)

                .when()
                .get("/{thingId}/attributes", thingId.toString())

                .then()
                .statusCode(HttpStatus.SC_REQUEST_TIMEOUT);

        assertDeviceHandledMessage(handledByDevice);
    }

    @Test
    public void liveRetrieveThingCommand() throws InterruptedException {
        final var correlationId = testNameCorrelationId.getCorrelationId();
        final var handledByDevice = new CountDownLatch(1);

        thingsWebSocketClient.onSignal(signal -> {
            final var receivedDittoHeaders = signal.getDittoHeaders();

            softly.assertThat(signal).isInstanceOf(RetrieveThing.class);
            assertIsLiveWithCorrelationId(signal, correlationId);
            softly.assertThat(receivedDittoHeaders.isResponseRequired()).isTrue();

            final var retrieveThing = (RetrieveThing) signal;
            final var response = RetrieveThingResponse.of(retrieveThing.getEntityId(),
                    THING_RESOURCE.getThing().toJson(),
                    receivedDittoHeaders);

            thingsWebSocketClient.emit(response);
            handledByDevice.countDown();
        });

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")

                .when()
                .get("/{thingId}", thingId.toString())

                .then()
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(THING_RESOURCE.getThing().toJson()))
                .statusCode(HttpStatus.SC_OK);

        assertDeviceHandledMessage(handledByDevice);
    }

    @Test
    public void liveModifyThingCommand() throws InterruptedException {
        final var correlationId = testNameCorrelationId.getCorrelationId();

        final var queue = new LinkedTransferQueue<Signal<?>>();
        thingsWebSocketClient.onSignal(queue::offer);

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "false")
                .body(THING_RESOURCE.getThing().toJsonString())

                .when()
                .put("/{thingId}", thingId.toString())

                .then()
                .statusCode(HttpStatus.SC_ACCEPTED);

        final var signal = queue.take();

        softly.assertThat(signal).isInstanceOfSatisfying(ModifyThing.class, modifyThing -> {
            softly.assertThat(modifyThing.getThing()).isEqualTo(THING_RESOURCE.getThing());
            assertIsLiveWithCorrelationId(signal, correlationId);
        });
    }

    @Test
    public void liveDeleteThingCommand() throws InterruptedException {
        final var correlationId = testNameCorrelationId.getCorrelationId();

        final var queue = new LinkedTransferQueue<Signal<?>>();
        thingsWebSocketClient.onSignal(queue::offer);

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "false")

                .when()
                .delete("/{thingId}", thingId.toString())

                .then()
                .statusCode(HttpStatus.SC_ACCEPTED);

        final var signal = queue.take();

        softly.assertThat(signal).isInstanceOf(DeleteThing.class);
        assertIsLiveWithCorrelationId(signal, correlationId);

        final var thingsHttpClient = THINGS_HTTP_CLIENT_RESOURCE.getThingsClient();
        final var thingAfterLiveDelete =
                thingsHttpClient.getThing(thingId, correlationId.withSuffix("existence-check"));

        softly.assertThat(thingAfterLiveDelete).hasValue(THING_RESOURCE.getThing());
    }

    @Test
    public void liveCommandWithErrorResponse() throws InterruptedException {
        final var correlationId = testNameCorrelationId.getCorrelationId();
        final var tooManyRequests = 429;
        final var handledByDevice = new CountDownLatch(1);

        thingsWebSocketClient.onSignal(signal -> {
            final var receivedDittoHeaders = signal.getDittoHeaders();

            assertIsLiveWithCorrelationId(signal, correlationId);
            softly.assertThat(signal).isInstanceOf(ModifyAttribute.class);
            softly.assertThat(receivedDittoHeaders.isResponseRequired()).isTrue();

            final var error = TooManyRequestsException.fromMessage("I'm just a random error mate!",
                    receivedDittoHeaders);
            final var errorResponse = ThingErrorResponse.of(error);

            thingsWebSocketClient.emit(errorResponse);
            handledByDevice.countDown();
        });

        LOGGER.info("Fire live command via Http with response");
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), DEVICE_TIMEOUT_STRING)
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .statusCode(tooManyRequests);

        assertDeviceHandledMessage(handledByDevice);
    }

    /*
     * Device does not want a response to the invalid response.
     */
    @Test
    public void liveCommandWithCustomStatusCodeResponse() throws InterruptedException {
        final var correlationId = testNameCorrelationId.getCorrelationId();
        final var protocolAdapter = DittoProtocolAdapter.of(HeaderTranslator.empty());
        final var handledByDevice = new CountDownLatch(1);

        thingsWebSocketClient.onSignal(signal -> {
            final var receivedDittoHeaders = signal.getDittoHeaders();

            assertIsLiveWithCorrelationId(signal, correlationId);
            softly.assertThat(signal).isInstanceOf(ModifyAttribute.class);
            softly.assertThat(receivedDittoHeaders.isResponseRequired()).isTrue();

            final var modifyAttribute = (ModifyAttribute) signal;

            final var response = ModifyAttributeResponse.modified(modifyAttribute.getEntityId(),
                    modifyAttribute.getAttributePointer(),
                    receivedDittoHeaders);

            var responseAdaptable = protocolAdapter.toAdaptable(response);
            responseAdaptable = ProtocolFactory.newAdaptableBuilder(responseAdaptable)
                    .withPayload(Payload.newBuilder(responseAdaptable.getPayload())
                            .withStatus(org.eclipse.ditto.base.model.common.HttpStatus.PARTIAL_CONTENT)
                            .build())
                    .build();

            thingsWebSocketClient.sendAdaptable(responseAdaptable);
            handledByDevice.countDown();
        });

        LOGGER.info("Fire live command via Http with response");
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), DEVICE_TIMEOUT_STRING)
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_REQUEST_TIMEOUT);

        assertDeviceHandledMessage(handledByDevice);
    }

    /*
     * Device expects a response to the invalid response.
     */
    @Test
    public void liveCommandWithCustomStatusCodeResponse2() throws InterruptedException {
        final var correlationId = testNameCorrelationId.getCorrelationId();
        final var protocolAdapter = DittoProtocolAdapter.of(HeaderTranslator.empty());
        final var handledByDeviceCountDown = new CountDownLatch(2);

        thingsWebSocketClient.onSignal(signal -> {
            final var invalidHttpStatus = org.eclipse.ditto.base.model.common.HttpStatus.PARTIAL_CONTENT;
            assertIsLiveWithCorrelationId(signal, correlationId);
            if (2 == handledByDeviceCountDown.getCount()) {
                final var receivedDittoHeaders = signal.getDittoHeaders();

                softly.assertThat(signal).isInstanceOf(ModifyAttribute.class);
                softly.assertThat(receivedDittoHeaders.isResponseRequired()).isTrue();

                final var modifyAttribute = (ModifyAttribute) signal;

                final var response = ModifyAttributeResponse.modified(modifyAttribute.getEntityId(),
                        modifyAttribute.getAttributePointer(),
                        receivedDittoHeaders);

                var responseAdaptable = protocolAdapter.toAdaptable(response);
                responseAdaptable = ProtocolFactory.newAdaptableBuilder(responseAdaptable)
                        .withPayload(Payload.newBuilder(responseAdaptable.getPayload())
                                .withStatus(invalidHttpStatus)
                                .build())
                        .withHeaders(receivedDittoHeaders)
                        .build();

                thingsWebSocketClient.sendAdaptable(responseAdaptable);
                handledByDeviceCountDown.countDown();
            } else if (1 == handledByDeviceCountDown.getCount()) {
                softly.assertThat(signal)
                        .as("is error response")
                        .isInstanceOfSatisfying(ThingErrorResponse.class, thingErrorResponse -> {
                            softly.assertThat(thingErrorResponse.getDittoRuntimeException())
                                    .as("is IllegalAdaptableException")
                                    .isInstanceOf(IllegalAdaptableException.class)
                                    .hasMessageContaining("%s is invalid.", invalidHttpStatus);
                        });
                handledByDeviceCountDown.countDown();
            }
        });

        LOGGER.info("Fire live command via Http with response");
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), DEVICE_TIMEOUT_STRING)
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_REQUEST_TIMEOUT);

        assertDeviceHandledMessage(handledByDeviceCountDown);
    }

    @Test
    public void sendInvalidCommandResponseTypeFromDevice() {

        // Subscribe WebSocket client for expected live signal. Send invalid response.
        thingsWebSocketClient.onSignal(signal -> thingsWebSocketClient.emit(RetrieveThingResponse.of(thingId,
                THING_RESOURCE.getThing().toJson(),
                signal.getDittoHeaders())));

        // Send HTTP live request.
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(testNameCorrelationId.getCorrelationId()))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "true")
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), DEVICE_TIMEOUT_STRING)
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_REQUEST_TIMEOUT)
                .body(containsString(String.format(
                        "Received no appropriate live response within the specified timeout." +
                                " An invalid response was received, though: Type of live response <%s> is not related" +
                                " to type of command <%s>.",
                        RetrieveThingResponse.TYPE,
                        ModifyAttribute.TYPE)));

        // WebSocket connections do not have a connection log. Thus, this test case ends here.
    }

    @Test
    public void certainHeadersOfCommandAreNotForwardedToDevice() {
        final var attributeName = "manufacturer";
        final var attributeValue = JsonValue.of("ACME");
        final var xForwardedScheme = "X-Forwarded-Scheme";
        final var acceptEncoding = "Accept-Encoding";

        // Check live command at device.
        thingsWebSocketClient.onSignal(signal -> {
            softly.assertThat(signal.getType()).as("signal type").isEqualTo(RetrieveAttribute.TYPE);

            final var signalDittoHeaders = signal.getDittoHeaders();
            final Predicate<String> isAcceptEncoding = acceptEncoding::equalsIgnoreCase;
            final Predicate<String> isXForwardedScheme = xForwardedScheme::equalsIgnoreCase;

            softly.assertThat(signalDittoHeaders.keySet())
                    .as("unexpected headers")
                    .filteredOn(isAcceptEncoding.or(isXForwardedScheme))
                    .isEmpty();

            thingsWebSocketClient.emit(RetrieveAttributeResponse.of(thingId,
                    JsonPointer.of(attributeName),
                    attributeValue,
                    signalDittoHeaders));
        });

        // Send HTTP live request.
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(testNameCorrelationId.getCorrelationId()))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "true")
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), DEVICE_TIMEOUT_STRING)
                .header(xForwardedScheme, "http")
                .header(acceptEncoding, "gzip,deflate")

                .when()
                .get("/{thingId}/attributes/{attributePath}", thingId.toString(), attributeName)

                .then()
                .statusCode(HttpStatus.SC_OK)
                .body(Matchers.is(attributeValue.toString()));
    }

    @Test
    public void applyLiveChannelTimeoutStrategy() throws InterruptedException {
        final var correlationId = testNameCorrelationId.getCorrelationId();

        final var queue = new LinkedTransferQueue<Signal<?>>();
        thingsWebSocketClient.onSignal(queue::offer);

        RestAssured.given(getBasicThingsRequestSpecWithLiveQueryBuilder(correlationId)
                        .addQueryParam("live-channel-timeout-strategy", "use-twin")
                        .addQueryParam("timeout", "10s")
                        .build())
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .get("/{thingId}", thingId.toString())

                .then()
                .statusCode(HttpStatus.SC_OK)
                .header("channel", "twin");

        final var signal = queue.take();
        softly.assertThat(signal).isInstanceOf(RetrieveThing.class);
        assertIsLiveWithCorrelationId(signal, correlationId);
    }

    private static RequestSpecification getBasicThingsRequestSpecWithLiveQuery(final CorrelationId correlationId) {
        return getBasicThingsRequestSpecWithLiveQueryBuilder(correlationId).build();
    }

    private static RequestSpecBuilder getBasicThingsRequestSpecWithLiveQueryBuilder(final CorrelationId correlationId) {
        final var correlationIdHeader = correlationId.toHeader();

        return new RequestSpecBuilder()
                .setBaseUri(THINGS_BASE_URI_RESOURCE.getThingsBaseUriApi2())
                .addQueryParam(DittoHeaderDefinition.CHANNEL.getKey(), "live")
                .setAuth(RestAssured.oauth2(TEST_SOLUTION_RESOURCE.getAccessToken().getToken()))
                .setContentType(ContentType.JSON)
                .addHeader(correlationIdHeader.getName(), correlationIdHeader.getValue())
                .log(LogDetail.ALL);
    }

    private void assertIsLiveWithCorrelationId(final WithDittoHeaders signal, final CorrelationId correlationId) {
        final var dittoHeaders = signal.getDittoHeaders();
        softly.assertThat(dittoHeaders.getChannel()).as("DittoHeaders live channel").hasValue("live");
        softly.assertThat(dittoHeaders.getCorrelationId())
                .as("DittoHeaders correlation ID")
                .hasValue(correlationId.toString());
    }

    private void assertDeviceHandledMessage(final CountDownLatch handledByDeviceLatch) throws InterruptedException {
        softly.assertThat(handledByDeviceLatch.await(DEVICE_TIMEOUT, DEVICE_TIMEOUT_UNIT)).isTrue();
    }

}
