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

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.http.HttpStatus;
import org.assertj.core.api.JUnitSoftAssertions;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.translator.HeaderTranslator;
import org.eclipse.ditto.internal.utils.akka.logging.DittoLoggerFactory;
import org.eclipse.ditto.json.JsonField;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
import org.eclipse.ditto.testing.common.HttpHeader;
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
import org.eclipse.ditto.testing.common.policies.PoliciesHttpClient;
import org.eclipse.ditto.testing.common.policies.PolicyWithConnectionSubjectResource;
import org.eclipse.ditto.testing.common.things.ThingsHttpClient;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.ThingErrorResponse;
import org.eclipse.ditto.things.model.signals.commands.exceptions.ThingNotAccessibleException;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttributeResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThingResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;

/**
 * Connectivity system-tests for live http channel.
 * At the moment only 'amqp' is tested. Other connectivity implementations need a different setup and additional
 * {@link org.eclipse.ditto.testing.common.client.ditto_protocol.DittoProtocolClient} implementations.
 * The testcases themselves don't care about the connectivity channel.
 */
@RunIf(DockerEnvironment.class)
@AmqpClientResource.Config(connectionName = LiveHttpChannelAmqpIT.CONNECTION_NAME)
public final class LiveHttpChannelAmqpIT {

    static final String CONNECTION_NAME = "LiveHttpChannelAmqpIT";

    private static final Logger LOGGER = DittoLoggerFactory.getLogger(LiveHttpChannelAmqpIT.class);

    private static final TestConfig TEST_CONFIG = TestConfig.getInstance();

    private static final int DEVICE_TIMEOUT = 5;
    private static final TimeUnit DEVICE_TIMEOUT_UNIT = TimeUnit.SECONDS;

    @ClassRule(order = 0)
    public static final HttpToAmqpResource HTTP_TO_AMQP_RESOURCE = HttpToAmqpResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 1)
    public static final PolicyWithConnectionSubjectResource POLICY_WITH_CONNECTION_SUBJECT_RESOURCE =
            PolicyWithConnectionSubjectResource.newInstance(HTTP_TO_AMQP_RESOURCE.getPoliciesHttpClientResource(),
                    CONNECTION_NAME);


    private static URI thingsBaseUri;
    private static ThingsHttpClient thingsHttpClient;
    private static PoliciesHttpClient policiesHttpClient;
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
        policiesHttpClient = HTTP_TO_AMQP_RESOURCE.getPoliciesHttpClientResource().getPoliciesClient();
        amqpClient = HTTP_TO_AMQP_RESOURCE.getAmqpClientResource().getAmqpClient();
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    }

    @Before
    public void before() {
        final var newThing = getThing();
        thing = thingsHttpClient.postThingWithInitialPolicy(newThing,
                POLICY_WITH_CONNECTION_SUBJECT_RESOURCE.getPolicy(),
                testNameCorrelationId.getCorrelationId(".postThing"));

        thingId = thing.getEntityId().orElseThrow();
    }

    private static Thing getThing() {
        final var thingJsonProducer = new ThingJsonProducer();
        return thingJsonProducer.getThing();
    }

    @Test
    public void sendLiveModifyAttributeViaHttpAndRespondViaAmqp() throws InterruptedException {
        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        amqpClient.onSignal(adaptable -> {
            final var modifyAttributeResponse = ModifyAttributeResponse.modified(thingId,
                    JsonPointer.of("manufacturer"),
                    adaptable.getDittoHeaders());

            amqpClient.sendResponse(modifyAttributeResponse);
            handledByDevice.countDown();
        });

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(HttpHeader.TIMEOUT.getName(), "5s")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingId.toString(), "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);

        assertDeviceHandledMessage(handledByDevice);
    }

    @Test
    public void sendLiveRetrieveThingViaHttpAndRespondViaAmqp() throws InterruptedException {
        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        amqpClient.onSignal(adaptable -> {
            final var retrieveThingResponse = RetrieveThingResponse.of(thingId, thing,
                    null, null, adaptable.getDittoHeaders());

            amqpClient.sendResponse(retrieveThingResponse);
            handledByDevice.countDown();
        });

        final var policyId = PolicyId.of(thingId);
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        final var policy = policiesHttpClient.getPolicy(policyId, baseCorrelationId.withSuffix(".getPolicy"));
        final var adjustedPolicy = PoliciesModelFactory.newPolicyBuilder(policy.orElseThrow())
                .setRevokedPermissionsFor("DEFAULT", "thing", "/attributes/model",
                        "READ")
                .setRevokedPermissionsFor("DEFAULT", "thing",
                        "/features/Vehicle/properties/status", "READ")
                .setRevokedPermissionsFor("DEFAULT", "thing",
                        "/features/EnvironmentScanner/properties/location", "READ")
                .build();
        policiesHttpClient.putPolicy(policyId, adjustedPolicy, baseCorrelationId.withSuffix(".putAdjustedPolicy"));

        final var expectedThing = thing.toBuilder()
                .removeAttribute(JsonPointer.of("model"))
                .removeFeatureProperty("Vehicle", JsonPointer.of("status"))
                .removeFeatureProperty("EnvironmentScanner", JsonPointer.of("location"))
                .build();

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .header(HttpHeader.TIMEOUT.getName(), "30s")

                .when()
                .get("/{thingId}", thingId.toString())

                .then()
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(expectedThing.toJson()))
                .statusCode(HttpStatus.SC_OK);

        assertDeviceHandledMessage(handledByDevice);
    }

    @Test
    public void sendLiveRetrieveThingViaHttpAndRespondViaAmqpWithErrorResponse() throws InterruptedException {
        final var protocolAdapter = DittoProtocolAdapter.of(HeaderTranslator.empty());
        final var handledByDevice = new CountDownLatch(1);
        final var correlationId = testNameCorrelationId.getCorrelationId();

        amqpClient.onSignal(signal -> {
            final var dittoHeaders = signal.getDittoHeaders();

            final var thingNotAccessibleException = ThingNotAccessibleException.newBuilder(thingId)
                    .dittoHeaders(dittoHeaders)
                    .build();

            final var errorResponse = ThingErrorResponse.of(thingId, thingNotAccessibleException, dittoHeaders);

            final var responseAdaptable = protocolAdapter.toAdaptable(errorResponse);

            final var responseJson = ProtocolFactory.wrapAsJsonifiableAdaptable(responseAdaptable).toJson();
            final var headersMap = responseAdaptable.getDittoHeaders().asCaseSensitiveMap();
            headersMap.remove("channel");

            final var jsonHeaderFields = headersMap.entrySet().stream()
                    .map(es -> JsonField.newInstance(es.getKey(), JsonValue.of(es.getValue())))
                    .collect(Collectors.toList());

            final var responseJsonWithoutChannelHeader = responseJson.set(
                    JsonifiableAdaptable.JsonFields.HEADERS,
                    JsonObject.newBuilder().setAll(jsonHeaderFields).build()).toString();

            amqpClient.sendRaw(responseJsonWithoutChannelHeader, headersMap,
                    dittoHeaders.getCorrelationId().orElseThrow());
            handledByDevice.countDown();
        });

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.TIMEOUT.getName(), "5s")

                .when()
                .get("/{thingId}", thingId.toString())

                .then()
                .statusCode(HttpStatus.SC_NOT_FOUND)
                .log()
                .everything();

        LOGGER.info("got response");
        assertDeviceHandledMessage(handledByDevice);
    }

    private static RequestSpecification getBasicThingsRequestSpecWithLiveQuery(final CorrelationId correlationId) {
        final var correlationIdHeader = correlationId.toHeader();

        return new RequestSpecBuilder().setBaseUri(thingsBaseUri)
                .addQueryParam(DittoHeaderDefinition.CHANNEL.getKey(), "live")
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
