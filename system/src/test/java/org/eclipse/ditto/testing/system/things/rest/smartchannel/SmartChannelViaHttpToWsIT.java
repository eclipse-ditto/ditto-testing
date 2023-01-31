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
package org.eclipse.ditto.testing.system.things.rest.smartchannel;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpStatus;
import org.assertj.core.api.JUnitSoftAssertions;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.LiveChannelTimeoutStrategy;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.SubjectIdFactory;
import org.eclipse.ditto.testing.common.TestSolutionResource;
import org.eclipse.ditto.testing.common.ThingJsonProducer;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.config.ThingsBaseUriResource;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.correlationid.TestNameCorrelationId;
import org.eclipse.ditto.testing.common.matcher.BodyContainsOnlyExpectedJsonValueMatcher;
import org.eclipse.ditto.testing.common.policies.PoliciesHttpClient;
import org.eclipse.ditto.testing.common.policies.PoliciesHttpClientResource;
import org.eclipse.ditto.testing.common.things.ThingsHttpClient;
import org.eclipse.ditto.testing.common.things.ThingsHttpClientResource;
import org.eclipse.ditto.testing.common.ws.ThingsWebSocketClientResource;
import org.eclipse.ditto.testing.common.ws.ThingsWebsocketClient;
import org.eclipse.ditto.testing.system.things.rest.ThingResource;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveAttribute;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveAttributeResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveFeature;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThingResponse;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;

/**
 * WebSocket answered system-tests for "smart channel" providing
 * {@link org.eclipse.ditto.base.model.headers.DittoHeaderDefinition#LIVE_CHANNEL_CONDITION live channel conditions}
 * and being delegated to either the live or twin channel.
 */
public final class SmartChannelViaHttpToWsIT {

    private static final String MATCHING_LIVE_CHANNEL_CONDITION = "eq(attributes/manufacturer,'ACME')";
    private static final String NON_MATCHING_LIVE_CHANNEL_CONDITION = "eq(attributes/foo,'bar')";

    private static final int TEST_TIMEOUT = 10;
    private static final TimeUnit TEST_TIMEOUT_UNIT = TimeUnit.SECONDS;
    private static final int DEVICE_TIMEOUT = 5;
    private static final TimeUnit DEVICE_TIMEOUT_UNIT = TimeUnit.SECONDS;

    private static final TestConfig TEST_CONFIG = TestConfig.getInstance();

    @ClassRule(order = 0)
    public static final TestSolutionResource TEST_SOLUTION_RESOURCE = TestSolutionResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 0)
    public static final TestSolutionResource TEST_SOLUTION_WS_RESOURCE = TestSolutionResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 1)
    public static final ThingsHttpClientResource THINGS_HTTP_CLIENT_RESOURCE =
            ThingsHttpClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @ClassRule(order = 1)
    public static final PoliciesHttpClientResource POLICIES_HTTP_CLIENT_RESOURCE =
            PoliciesHttpClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @ClassRule(order = 1)
    public static final ThingsWebSocketClientResource WS_CLIENT_RESOURCE =
            ThingsWebSocketClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_WS_RESOURCE);

    @ClassRule(order = 2)
    public static final ThingResource THING_RESOURCE = ThingResource.fromThingJsonProducer(THINGS_HTTP_CLIENT_RESOURCE);

    @ClassRule
    public static final ThingsBaseUriResource THINGS_BASE_URI_RESOURCE = ThingsBaseUriResource.newInstance(TEST_CONFIG);

    private static PoliciesHttpClient policiesHttpClient;
    private static ThingsHttpClient thingsHttpClient;
    private static ThingsWebsocketClient thingsWebSocketClient;

    @BeforeClass
    public static void beforeClass() {
        policiesHttpClient = POLICIES_HTTP_CLIENT_RESOURCE.getPoliciesClient();
        thingsHttpClient = THINGS_HTTP_CLIENT_RESOURCE.getThingsClient();
        thingsWebSocketClient = WS_CLIENT_RESOURCE.getThingsWebsocketClient();
        thingsWebSocketClient.sendProtocolCommand("START-SEND-LIVE-COMMANDS", "START-SEND-LIVE-COMMANDS:ACK").join();
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    }

    @Rule
    public final TestNameCorrelationId testNameCorrelationId = TestNameCorrelationId.newInstance();

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    @Rule
    public final Timeout timeout = new Timeout(TEST_TIMEOUT, TEST_TIMEOUT_UNIT);

    private Thing thing;
    private ThingId thingId;

    @Before
    public void before() {
        final var newThing = new ThingJsonProducer().getThing();
        thing = thingsHttpClient.postThing(newThing,
                testNameCorrelationId.getCorrelationId(".postThing"));

        thingId = thing.getEntityId().orElseThrow();
        final var policyId = PolicyId.of(thingId);
        final var correlationId = CorrelationId.random();
        final var policy = policiesHttpClient.getPolicy(policyId, correlationId.withSuffix(".beforeClass"));
        final var adjustedPolicy = PoliciesModelFactory.newPolicyBuilder(policy.orElseThrow())
                .setSubjectFor("WS",
                        Subject.newInstance(SubjectIdFactory.getSubjectIdForUsername(TEST_SOLUTION_WS_RESOURCE.getTestUsername())))
                .setGrantedPermissionsFor("WS", "thing", "/", "READ", "WRITE")
                .build();
        policiesHttpClient.putPolicy(policyId, adjustedPolicy, correlationId.withSuffix(".putAdjustedPolicy"));
    }

    @Test
    public void retrieveThingViaHttpLiveChannelConditionRespondViaWs() throws InterruptedException {
        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        final var returnedLiveThing = Thing.newBuilder()
                .setAttribute(JsonPointer.of("manufacturer"), JsonValue.of("live-ACME"))
                .setId(thingId)
                .build();
        thingsWebSocketClient.onSignal(signal -> {
            softly.assertThat(signal).isInstanceOf(RetrieveThing.class);

            final var retrieveThingResponse = RetrieveThingResponse.of(thingId, returnedLiveThing,
                    null, null, signal.getDittoHeaders());

            thingsWebSocketClient.emit(retrieveThingResponse);
            handledByDevice.countDown();
        });

        RestAssured.given(
                        getBasicThingsRequestSpecWithLiveChannelCondition(MATCHING_LIVE_CHANNEL_CONDITION, correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(HttpHeader.TIMEOUT.getName(), "5s")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_TIMEOUT_STRATEGY.getKey(),
                        LiveChannelTimeoutStrategy.FAIL.toString())

                .when()
                .get("/{thingId}", thingId.toString())

                .then()
                .header(HttpHeader.CONTENT_TYPE.getName(), "application/json")
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "false")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_CONDITION_MATCHED.getKey(), "true")
                .header(DittoHeaderDefinition.CHANNEL.getKey(), "live")
                .header(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId.toString())
                .header(DittoHeaderDefinition.ORIGINATOR.getKey(), Matchers.startsWithIgnoringCase(
                        SubjectIdFactory.getSubjectIdForUsername(TEST_SOLUTION_WS_RESOURCE.getTestUsername()).toString()))
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(returnedLiveThing.toJson()))
                .statusCode(HttpStatus.SC_OK);

        assertDeviceHandledMessage(handledByDevice);
        softly.assertAll();
    }

    @Test
    public void retrieveAttributePropertyViaHttpLiveChannelConditionRespondViaWs() throws InterruptedException {
        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        final var returnedLiveAttribute = JsonValue.of("live-ACME");
        thingsWebSocketClient.onSignal(signal -> {
            softly.assertThat(signal).isInstanceOf(RetrieveAttribute.class);

            final var retrieveAttributeResponse = RetrieveAttributeResponse.of(thingId,
                    JsonPointer.of("manufacturer"),
                    returnedLiveAttribute,
                    signal.getDittoHeaders());

            thingsWebSocketClient.emit(retrieveAttributeResponse);
            handledByDevice.countDown();
        });

        RestAssured.given(
                        getBasicThingsRequestSpecWithLiveChannelCondition(MATCHING_LIVE_CHANNEL_CONDITION, correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(HttpHeader.TIMEOUT.getName(), "5s")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_TIMEOUT_STRATEGY.getKey(),
                        LiveChannelTimeoutStrategy.FAIL.toString())

                .when()
                .get("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .header(HttpHeader.CONTENT_TYPE.getName(), "application/json")
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "false")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_CONDITION_MATCHED.getKey(), "true")
                .header(DittoHeaderDefinition.CHANNEL.getKey(), "live")
                .header(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId.toString())
                .header(DittoHeaderDefinition.ORIGINATOR.getKey(), Matchers.startsWithIgnoringCase(
                        SubjectIdFactory.getSubjectIdForUsername(TEST_SOLUTION_WS_RESOURCE.getTestUsername()).toString()))
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(returnedLiveAttribute))
                .statusCode(HttpStatus.SC_OK);

        assertDeviceHandledMessage(handledByDevice);
        softly.assertAll();
    }

    @Test
    public void retrieveAttributePropertyViaHttpNonMatchingLiveChannelCondition() throws InterruptedException {
        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        thingsWebSocketClient.onSignal(adaptable -> handledByDevice.countDown());

        RestAssured.given(
                        getBasicThingsRequestSpecWithLiveChannelCondition(NON_MATCHING_LIVE_CHANNEL_CONDITION, correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(HttpHeader.TIMEOUT.getName(), "5s")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_TIMEOUT_STRATEGY.getKey(),
                        LiveChannelTimeoutStrategy.FAIL.toString())

                .when()
                .get("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .header(HttpHeader.CONTENT_TYPE.getName(), "application/json")
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "false")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_CONDITION_MATCHED.getKey(), "false")
                .header(DittoHeaderDefinition.CHANNEL.getKey(), "twin")
                .header(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId.toString())
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(JsonValue.of("ACME")))
                .statusCode(HttpStatus.SC_OK);

        assertDeviceDidNotHandleMessage(handledByDevice);
        softly.assertAll();
    }

    @Test
    public void retrieveThingViaHttpLiveChannelConditionTimeoutWithoutTwinFallback() throws InterruptedException {
        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        thingsWebSocketClient.onSignal(signal -> {
            softly.assertThat(signal).isInstanceOf(RetrieveThing.class);
            handledByDevice.countDown();
        });

        RestAssured.given(
                        getBasicThingsRequestSpecWithLiveChannelCondition(MATCHING_LIVE_CHANNEL_CONDITION, correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(HttpHeader.TIMEOUT.getName(), "5s")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_TIMEOUT_STRATEGY.getKey(),
                        LiveChannelTimeoutStrategy.FAIL.toString())

                .when()
                .get("/{thingId}", thingId.toString())

                .then()
                .header(HttpHeader.CONTENT_TYPE.getName(), "application/json")
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "false")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_CONDITION_MATCHED.getKey(), "true")
                .header(DittoHeaderDefinition.CHANNEL.getKey(), "live")
                .header(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId.toString())
                .statusCode(HttpStatus.SC_REQUEST_TIMEOUT);

        assertDeviceHandledMessage(handledByDevice);
    }

    @Test
    public void retrieveAttributeViaHttpLiveChannelConditionTimeoutWithoutTwinFallback()
            throws InterruptedException {

        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        thingsWebSocketClient.onSignal(signal -> {
            softly.assertThat(signal).isInstanceOf(RetrieveAttribute.class);
            handledByDevice.countDown();
        });

        RestAssured.given(
                        getBasicThingsRequestSpecWithLiveChannelCondition(MATCHING_LIVE_CHANNEL_CONDITION, correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(HttpHeader.TIMEOUT.getName(), "5s")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_TIMEOUT_STRATEGY.getKey(),
                        LiveChannelTimeoutStrategy.FAIL.toString())

                .when()
                .get("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .header(HttpHeader.CONTENT_TYPE.getName(), "application/json")
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "false")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_CONDITION_MATCHED.getKey(), "true")
                .header(DittoHeaderDefinition.CHANNEL.getKey(), "live")
                .header(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId.toString())
                .statusCode(HttpStatus.SC_REQUEST_TIMEOUT);

        assertDeviceHandledMessage(handledByDevice);
    }

    @Test
    public void retrieveFeatureViaHttpLiveChannelConditionInvalidResponseWithoutTwinFallback()
            throws InterruptedException {

        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        thingsWebSocketClient.onSignal(signal -> {
            softly.assertThat(signal).isInstanceOf(RetrieveFeature.class);

            final var invalidResponseType = RetrieveAttributeResponse.of(thingId,
                    JsonPointer.of("manufacturer"),
                    JsonValue.of("wroooong"),
                    signal.getDittoHeaders());

            thingsWebSocketClient.emit(invalidResponseType);
            handledByDevice.countDown();
        });

        RestAssured.given(
                        getBasicThingsRequestSpecWithLiveChannelCondition(MATCHING_LIVE_CHANNEL_CONDITION, correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(HttpHeader.TIMEOUT.getName(), "5s")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_TIMEOUT_STRATEGY.getKey(),
                        LiveChannelTimeoutStrategy.FAIL.toString())

                .when()
                .get("/{thingId}/features/{featureId}", thingId.toString(), "Vehicle")

                .then()
                .header(HttpHeader.CONTENT_TYPE.getName(), "application/json")
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "false")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_CONDITION_MATCHED.getKey(), "true")
                .header(DittoHeaderDefinition.CHANNEL.getKey(), "live")
                .header(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId.toString())
                .statusCode(HttpStatus.SC_REQUEST_TIMEOUT);

        assertDeviceHandledMessage(handledByDevice);
    }

    @Test
    public void retrieveThingViaHttpLiveChannelConditionTimeoutWithTwinFallback() throws InterruptedException {
        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        thingsWebSocketClient.onSignal(signal -> handledByDevice.countDown());

        RestAssured.given(
                        getBasicThingsRequestSpecWithLiveChannelCondition(MATCHING_LIVE_CHANNEL_CONDITION, correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(HttpHeader.TIMEOUT.getName(), "5s")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_TIMEOUT_STRATEGY.getKey(),
                        LiveChannelTimeoutStrategy.USE_TWIN.toString())

                .when()
                .get("/{thingId}", thingId.toString())

                .then()
                .header(HttpHeader.CONTENT_TYPE.getName(), "application/json")
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "false")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_CONDITION_MATCHED.getKey(), "true")
                .header(DittoHeaderDefinition.CHANNEL.getKey(), "twin")
                .header(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId.toString())
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(thing.toJson()))
                .statusCode(HttpStatus.SC_OK);

        assertDeviceHandledMessage(handledByDevice);
    }

    @Test
    public void retrieveAttributePropertyViaHttpLiveChannelConditionTimeoutWithTwinFallback()
            throws InterruptedException {

        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        thingsWebSocketClient.onSignal(signal -> handledByDevice.countDown());

        RestAssured.given(
                        getBasicThingsRequestSpecWithLiveChannelCondition(MATCHING_LIVE_CHANNEL_CONDITION, correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(HttpHeader.TIMEOUT.getName(), "5s")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_TIMEOUT_STRATEGY.getKey(),
                        LiveChannelTimeoutStrategy.USE_TWIN.toString())

                .when()
                .get("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .header(HttpHeader.CONTENT_TYPE.getName(), "application/json")
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "false")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_CONDITION_MATCHED.getKey(), "true")
                .header(DittoHeaderDefinition.CHANNEL.getKey(), "twin")
                .header(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId.toString())
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(JsonValue.of("ACME")))
                .statusCode(HttpStatus.SC_OK);

        assertDeviceHandledMessage(handledByDevice);
    }

    private static RequestSpecification getBasicThingsRequestSpecWithLiveChannelCondition(
            final String liveChannelCondition,
            final CorrelationId correlationId) {
        final var correlationIdHeader = correlationId.toHeader();

        return new RequestSpecBuilder()
                .setBaseUri(THINGS_BASE_URI_RESOURCE.getThingsBaseUriApi2())
                .addQueryParam(DittoHeaderDefinition.LIVE_CHANNEL_CONDITION.getKey(), liveChannelCondition)
                .setAuth(RestAssured.oauth2(TEST_SOLUTION_RESOURCE.getAccessToken().getToken()))
                .setContentType(ContentType.JSON)
                .addHeader(correlationIdHeader.getName(), correlationIdHeader.getValue())
                .build();
    }

    private void assertDeviceHandledMessage(final CountDownLatch handledByDeviceLatch) throws InterruptedException {
        softly.assertThat(handledByDeviceLatch.await(DEVICE_TIMEOUT, DEVICE_TIMEOUT_UNIT)).isTrue();
    }

    private void assertDeviceDidNotHandleMessage(final CountDownLatch handledByDeviceLatch)
            throws InterruptedException {
        softly.assertThat(handledByDeviceLatch.await(DEVICE_TIMEOUT, DEVICE_TIMEOUT_UNIT)).isFalse();
    }

}
