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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.apache.http.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.TestSolutionResource;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.config.ThingsBaseUriResource;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.correlationid.TestNameCorrelationId;
import org.eclipse.ditto.testing.common.matcher.BodyContainsOnlyExpectedJsonValueMatcher;
import org.eclipse.ditto.testing.common.policies.PoliciesHttpClient;
import org.eclipse.ditto.testing.common.policies.PoliciesHttpClientResource;
import org.eclipse.ditto.testing.common.things.ThingsHttpClientResource;
import org.eclipse.ditto.testing.common.ws.ThingsWebSocketClientResource;
import org.eclipse.ditto.testing.common.ws.ThingsWebsocketClient;
import org.eclipse.ditto.testing.system.things.rest.ThingResource;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttributes;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttributesResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveAttributes;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveAttributesResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThingResponse;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;

public final class LiveHttpChannelWithPermissionCheckIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(LiveHttpChannelWithPermissionCheckIT.class);

    private static final TestConfig TEST_CONFIG = TestConfig.getInstance();

    @ClassRule(order = 0)
    public static final TestSolutionResource TEST_SOLUTION_RESOURCE = TestSolutionResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 1)
    public static final ThingsHttpClientResource THINGS_HTTP_CLIENT_RESOURCE =
            ThingsHttpClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @ClassRule(order = 1)
    public static final PoliciesHttpClientResource POLICIES_HTTP_CLIENT_RESOURCE =
            PoliciesHttpClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @ClassRule(order = 1)
    public static final ThingsWebSocketClientResource WS_CLIENT_RESOURCE =
            ThingsWebSocketClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @ClassRule
    public static final ThingsBaseUriResource THINGS_BASE_URI_RESOURCE = ThingsBaseUriResource.newInstance(TEST_CONFIG);

    private static PoliciesHttpClient policiesHttpClient;
    private static ThingsWebsocketClient thingsWebsocketClient;

    @Rule
    public final TestNameCorrelationId testNameCorrelationId = TestNameCorrelationId.newInstance();

    @Rule
    public final ThingResource thingResource = ThingResource.fromThingJsonProducer(THINGS_HTTP_CLIENT_RESOURCE);

    private Thing thing;
    private ThingId thingId;

    @BeforeClass
    public static void beforeClass() {
        policiesHttpClient = POLICIES_HTTP_CLIENT_RESOURCE.getPoliciesClient();
        thingsWebsocketClient = WS_CLIENT_RESOURCE.getThingsWebsocketClient();
        thingsWebsocketClient.sendProtocolCommand("START-SEND-LIVE-COMMANDS", "START-SEND-LIVE-COMMANDS:ACK").join();
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    }

    @Before
    public void before() {
        thing = thingResource.getThing();
        thingId = thingResource.getThingId();
    }

    @Test
    public void sendLiveRetrieveThingCommandWithoutPermission() {
        final var policyId = PolicyId.of(thingId);
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        final var policy = policiesHttpClient.getPolicy(policyId, baseCorrelationId.withSuffix(".getPolicy"));
        final var adjustedPolicy = PoliciesModelFactory.newPolicyBuilder(policy.orElseThrow())
                .setRevokedPermissionsFor("DEFAULT", "thing", "/",
                        "READ", "WRITE")
                .build();
        policiesHttpClient.putPolicy(policyId, adjustedPolicy, baseCorrelationId.withSuffix(".putAdjustedPolicy"));

        LOGGER.info("Fire live command via Http with response");
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")

                .when()
                .get("/{thingId}", thingId.toString())

                .then()
                .statusCode(HttpStatus.SC_NOT_FOUND);
    }

    @Test
    public void sendLiveModifyAttributesCommandWithPartialPermission() {
        thingsWebsocketClient.onSignal(signal -> {
            assertThat(signal).isInstanceOf(ModifyAttributes.class);
            assertThat(signal.getDittoHeaders().getChannel()).hasValue("live");

            final var modifyAttributes = (ModifyAttributes) signal;
            final var modifyAttributesResponse =
                    ModifyAttributesResponse.modified(modifyAttributes.getEntityId(), modifyAttributes.getDittoHeaders());

            thingsWebsocketClient.emit(modifyAttributesResponse);
        });

        final var policyId = PolicyId.of(thingId);
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        final var policy = policiesHttpClient.getPolicy(policyId, baseCorrelationId.withSuffix(".getPolicy"));
        final var adjustedPolicy = PoliciesModelFactory.newPolicyBuilder(policy.orElseThrow())
                .setRevokedPermissionsFor("DEFAULT", "thing", "/features",
                        "READ", "WRITE")
                .build();
        policiesHttpClient.putPolicy(policyId, adjustedPolicy, baseCorrelationId.withSuffix(".putAdjustedPolicy"));

        final var attributes = Attributes.newBuilder()
                .set("status", "active")
                .set("failure", false)
                .build();

        LOGGER.info("Fire live command via Http with response");
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .body(attributes.toJsonString())

                .when()
                .put("/{thingId}/attributes", thingId.toString())

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void sendLiveRetrieveAttributesWithRestrictedPermission() {
        final var knownAttributeKey = "secretAttribute";
        final var attributes = Attributes.newBuilder()
                .set("status", "active")
                .set("failure", false)
                .set(knownAttributeKey, "verySecret")
                .build();

        thingsWebsocketClient.onSignal(signal -> {
            assertThat(signal).isInstanceOf(RetrieveAttributes.class);
            assertThat(signal.getDittoHeaders().getChannel()).hasValue("live");

            final var retrieveAttributesAttribute = (RetrieveAttributes) signal;
            final var response =
                    RetrieveAttributesResponse.of(retrieveAttributesAttribute.getEntityId(), attributes,
                            retrieveAttributesAttribute.getDittoHeaders());

            thingsWebsocketClient.emit(response);
        });

        final var policyId = PolicyId.of(thingId);
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        final var policy = policiesHttpClient.getPolicy(policyId, baseCorrelationId.withSuffix(".getPolicy"));
        final var adjustedPolicy = PoliciesModelFactory.newPolicyBuilder(policy.orElseThrow())
                .setRevokedPermissionsFor("DEFAULT", "thing", "/attributes/" + knownAttributeKey,
                        "READ")
                .build();
        policiesHttpClient.putPolicy(policyId, adjustedPolicy, baseCorrelationId.withSuffix(".putAdjustedPolicy"));

        final var expectedAttributes = attributes.toBuilder().remove(knownAttributeKey).build();

        LOGGER.info("Fire live command via Http with response");
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(testNameCorrelationId.getCorrelationId()))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")

                .when()
                .get("/{thingId}/attributes", thingId.toString())

                .then()
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(expectedAttributes.toJson()))
                .statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void sendLiveRetrieveThingWithRestrictedPermission() {
        thingsWebsocketClient.onSignal(signal -> {
            assertThat(signal).isInstanceOf(RetrieveThing.class);
            assertThat(signal.getDittoHeaders().getChannel()).hasValue("live");

            final var retrieveThing = (RetrieveThing) signal;
            final var retrieveThingResponse =
                    RetrieveThingResponse.of(retrieveThing.getEntityId(), thing.toJson(),
                            retrieveThing.getDittoHeaders());

            thingsWebsocketClient.emit(retrieveThingResponse);
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

        LOGGER.info("Fire live command via Http with response");
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(testNameCorrelationId.getCorrelationId()))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.name(), "true")

                .when()
                .get("/{thingId}", thingId.toString())

                .then()
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(expectedThing.toJson()))
                .statusCode(HttpStatus.SC_OK);
    }

    private static RequestSpecification getBasicThingsRequestSpecWithLiveQuery(final CorrelationId correlationId) {
        final var correlationIdHeader = correlationId.toHeader();

        return new RequestSpecBuilder()
                .setBaseUri(THINGS_BASE_URI_RESOURCE.getThingsBaseUriApi2())
                .addQueryParam(DittoHeaderDefinition.CHANNEL.getKey(), "live")
                .setAuth(RestAssured.oauth2(TEST_SOLUTION_RESOURCE.getAccessToken().getToken()))
                .setContentType(ContentType.JSON)
                .addHeader(correlationIdHeader.getName(), correlationIdHeader.getValue())
                .build();
    }

}
