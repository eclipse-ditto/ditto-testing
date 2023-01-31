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

import java.net.URI;
import java.text.MessageFormat;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpStatus;
import org.assertj.core.api.JUnitSoftAssertions;
import org.eclipse.ditto.base.model.auth.AuthorizationSubject;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.LiveChannelTimeoutStrategy;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.TestSolutionResource;
import org.eclipse.ditto.testing.common.ThingJsonProducer;
import org.eclipse.ditto.testing.common.composite_resources.HttpToAmqpResource;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.connectivity.amqp.AmqpClient;
import org.eclipse.ditto.testing.common.connectivity.amqp.AmqpClientResource;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.correlationid.TestNameCorrelationId;
import org.eclipse.ditto.testing.common.gateway.GatewayConfig;
import org.eclipse.ditto.testing.common.matcher.BodyContainsOnlyExpectedJsonValueMatcher;
import org.eclipse.ditto.testing.common.policies.PolicyWithConnectionSubjectResource;
import org.eclipse.ditto.testing.common.things.ThingsHttpClient;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveAttribute;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveAttributeResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThingResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;

/**
 * Connectivity system-tests for "smart channel" providing
 * {@link org.eclipse.ditto.base.model.headers.DittoHeaderDefinition#LIVE_CHANNEL_CONDITION live channel conditions}
 * and being delegated to either the live or twin channel.
 */
@RunIf(DockerEnvironment.class)
@AmqpClientResource.Config(connectionName = SmartChannelViaHttpToAmqpIT.CONNECTION_NAME)
public final class SmartChannelViaHttpToAmqpIT {

    static final String CONNECTION_NAME = "SmartChannelViaHttpToAmqpIT";

    private static final TestConfig TEST_CONFIG = TestConfig.getInstance();

    private static final int DEVICE_TIMEOUT = 5;
    private static final TimeUnit DEVICE_TIMEOUT_UNIT = TimeUnit.SECONDS;


    @ClassRule(order = 0)
    public static final TestSolutionResource TEST_SOLUTION_RESOURCE = TestSolutionResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 1)
    public static final HttpToAmqpResource HTTP_TO_AMQP_RESOURCE = HttpToAmqpResource.newInstance(TEST_CONFIG,
            TEST_SOLUTION_RESOURCE);

    @ClassRule(order = 2)
    public static final PolicyWithConnectionSubjectResource POLICY_WITH_CONNECTION_SUBJECT_RESOURCE =
            PolicyWithConnectionSubjectResource.newInstance(TEST_SOLUTION_RESOURCE,
                    HTTP_TO_AMQP_RESOURCE.getPoliciesHttpClientResource(), CONNECTION_NAME);

    private static final String MATCHING_LIVE_CHANNEL_CONDITION = "eq(attributes/manufacturer,'ACME')";
    private static final String NON_MATCHING_LIVE_CHANNEL_CONDITION = "eq(attributes/foo,'bar')";

    private static URI thingsBaseUri;
    private static ThingsHttpClient thingsHttpClient;
    private static AmqpClient amqpClient;

    private Thing thing;
    private ThingId thingId;

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    @Rule
    public final TestNameCorrelationId testNameCorrelationId = TestNameCorrelationId.newInstance();

    @BeforeClass
    public static void beforeClass() {
        final var gatewayConfig = GatewayConfig.of(TEST_CONFIG);
        final var httpUriApi2 = gatewayConfig.getHttpUriApi2();
        thingsBaseUri = httpUriApi2.resolve("./things");
        thingsHttpClient = HTTP_TO_AMQP_RESOURCE.getThingsHttpClientResource().getThingsClient();
        amqpClient = HTTP_TO_AMQP_RESOURCE.getAmqpClientResource().getAmqpClient();
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    }

    @Before
    public void before() {
        final var newThing = new ThingJsonProducer().getThing();
        thing = thingsHttpClient.postThingWithInitialPolicy(newThing,
                POLICY_WITH_CONNECTION_SUBJECT_RESOURCE.getPolicy(),
                testNameCorrelationId.getCorrelationId(".postThing"));

        thingId = thing.getEntityId().orElseThrow();
    }

    @Test
    public void retrieveThingViaHttpLiveChannelConditionRespondViaAmqp() throws InterruptedException {
        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        final var attributeLiveValue = JsonValue.of("live-ACME-" + correlationId);
        final var returnedLiveThing = Thing.newBuilder()
                .setAttribute(JsonPointer.of("manufacturer"), attributeLiveValue)
                .setId(thingId)
                .build();
        amqpClient.onSignal(signal -> {
            softly.assertThat(signal).isInstanceOf(RetrieveThing.class);

            final var retrieveThingResponse = RetrieveThingResponse.of(thingId, returnedLiveThing,
                    null, null, signal.getDittoHeaders());

            amqpClient.sendResponse(retrieveThingResponse);
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
                .header(DittoHeaderDefinition.ORIGINATOR.getKey(), getOriginator())
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(returnedLiveThing.toJson()))
                .statusCode(HttpStatus.SC_OK);

        // also check that after the live response was consumed, the twin also was updated to the live value:
        softly.assertThat(
                thingsHttpClient.getAttribute(thingId, "manufacturer", correlationId.withSuffix(".checkTwin"))
        ).contains(attributeLiveValue);

        assertDeviceHandledMessage(handledByDevice);
        softly.assertAll();
    }

    @Test
    public void retrieveThingViaHttpNonMatchingLiveChannelCondition() throws InterruptedException {
        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        amqpClient.onSignal(signal -> handledByDevice.countDown());

        RestAssured.given(
                getBasicThingsRequestSpecWithLiveChannelCondition(NON_MATCHING_LIVE_CHANNEL_CONDITION, correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(HttpHeader.TIMEOUT.getName(), "5s")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_TIMEOUT_STRATEGY.getKey(),
                        LiveChannelTimeoutStrategy.FAIL.toString())

                .when()
                .get("/{thingId}", thingId.toString())

                .then()
                .header(HttpHeader.CONTENT_TYPE.getName(), "application/json")
                .header(DittoHeaderDefinition.LIVE_CHANNEL_CONDITION_MATCHED.getKey(), "false")
                .header(DittoHeaderDefinition.CHANNEL.getKey(), "twin")
                .header(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId.toString())
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(thing.toJson()))
                .statusCode(HttpStatus.SC_OK);

        assertDeviceDidNotHandleMessage(handledByDevice);
        softly.assertAll();
    }

    @Test
    public void retrieveAttributePropertyViaHttpLiveChannelConditionRespondViaAmqp() throws InterruptedException {
        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        final var attributeLiveValue = JsonValue.of("live-ACME-" + correlationId);
        amqpClient.onSignal(signal -> {
            softly.assertThat(signal).isInstanceOf(RetrieveAttribute.class);

            final var retrieveThingResponse = RetrieveAttributeResponse.of(thingId,
                    JsonPointer.of("manufacturer"),
                    attributeLiveValue,
                    signal.getDittoHeaders());

            amqpClient.sendResponse(retrieveThingResponse);
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
                .header(DittoHeaderDefinition.LIVE_CHANNEL_CONDITION_MATCHED.getKey(), "true")
                .header(DittoHeaderDefinition.CHANNEL.getKey(), "live")
                .header(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId.toString())
                .header(DittoHeaderDefinition.ORIGINATOR.getKey(),getOriginator())
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(attributeLiveValue))
                .statusCode(HttpStatus.SC_OK);

        // also check that after the live response was consumed, the twin also was updated to the live value:
        softly.assertThat(
                thingsHttpClient.getAttribute(thingId, "manufacturer", correlationId.withSuffix(".checkTwin"))
        ).contains(attributeLiveValue);

        assertDeviceHandledMessage(handledByDevice);
        softly.assertAll();
    }

    @Test
    public void retrieveThingViaHttpLiveChannelConditionTimeoutWithoutTwinFallback() throws InterruptedException {
        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        amqpClient.onSignal(signal -> handledByDevice.countDown());

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
        softly.assertAll();
    }

    @Test
    public void retrieveAttributePropertyViaHttpLiveChannelConditionTimeoutWithoutTwinFallback()
            throws InterruptedException {

        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        amqpClient.onSignal(signal -> handledByDevice.countDown());

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
        softly.assertAll();
    }

    @Test
    public void retrieveThingViaHttpLiveChannelConditionTimeoutWithTwinFallback() throws InterruptedException {
        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        amqpClient.onSignal(signal -> handledByDevice.countDown());

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
        softly.assertAll();
    }

    @Test
    public void retrieveAttributePropertyViaHttpLiveChannelConditionTimeoutWithTwinFallback()
            throws InterruptedException {

        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        amqpClient.onSignal(signal -> handledByDevice.countDown());

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
        softly.assertAll();
    }

    private static String getOriginator() {
        final var testUsername = HTTP_TO_AMQP_RESOURCE.getTestSolutionResource().getTestUsername();
        final var authorizationSubject = AuthorizationSubject.newInstance(MessageFormat.format("integration:{0}:{1}",
                testUsername,
                CONNECTION_NAME));
        return authorizationSubject.getId();
    }

    private static RequestSpecification getBasicThingsRequestSpecWithLiveChannelCondition(
            final String liveChannelCondition,
            final CorrelationId correlationId) {

        final var correlationIdHeader = correlationId.toHeader();

        return new RequestSpecBuilder().setBaseUri(thingsBaseUri)
                .addQueryParam(DittoHeaderDefinition.LIVE_CHANNEL_CONDITION.getKey(), liveChannelCondition)
                .setAuth(RestAssured.oauth2(HTTP_TO_AMQP_RESOURCE.getTestSolutionResource().getAccessToken().getToken()))
                .setContentType(ContentType.JSON)
                .addHeader(correlationIdHeader.getName(), correlationIdHeader.getValue())
                .build();
    }

    private void assertDeviceHandledMessage(final CountDownLatch handledByDeviceLatch) throws InterruptedException {
        softly.assertThat(handledByDeviceLatch.await(DEVICE_TIMEOUT, DEVICE_TIMEOUT_UNIT)).isTrue();
    }

    private void assertDeviceDidNotHandleMessage(final CountDownLatch handledByDeviceLatch) throws InterruptedException {
        softly.assertThat(handledByDeviceLatch.await(DEVICE_TIMEOUT, DEVICE_TIMEOUT_UNIT)).isFalse();
    }

}
