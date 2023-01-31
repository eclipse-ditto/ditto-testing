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

import java.text.MessageFormat;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.JUnitSoftAssertions;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.policies.model.Permissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PolicyConstants;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.ResourceKey;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectType;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.SubjectIdFactory;
import org.eclipse.ditto.testing.common.TestSolutionResource;
import org.eclipse.ditto.testing.common.ThingJsonProducer;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.config.ThingsBaseUriResource;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.correlationid.TestNameCorrelationId;
import org.eclipse.ditto.testing.common.matcher.BodyContainsOnlyExpectedJsonValueMatcher;
import org.eclipse.ditto.testing.common.policies.PoliciesHttpClientResource;
import org.eclipse.ditto.testing.common.ws.ThingsWebSocketClientResource;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingConstants;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.exceptions.ThingNotCreatableException;
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
 * System test for the various scenarios for posting a thing.
 */
public final class LiveHttpChannelCreateThingIT {

    private static final TestConfig TEST_CONFIG = TestConfig.getInstance();

    @ClassRule(order = 0)
    public static final TestSolutionResource TEST_SOLUTION_RESOURCE = TestSolutionResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 1)
    public static final PoliciesHttpClientResource POLICIES_HTTP_CLIENT_RESOURCE =
            PoliciesHttpClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @ClassRule
    public static final ThingsBaseUriResource THINGS_BASE_URI_RESOURCE = ThingsBaseUriResource.newInstance(TEST_CONFIG);

    private static Thing thing;

    @Rule
    public final TestNameCorrelationId testNameCorrelationId = TestNameCorrelationId.newInstance();

    @Rule
    public final ThingsWebSocketClientResource wsClientResource =
            ThingsWebSocketClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    @Rule
    public final Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    @BeforeClass
    public static void beforeClass() {
        final var thingJsonProducer = new ThingJsonProducer();
        thing = thingJsonProducer.getThing();
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
    }

    @Before
    public void before() {
        final var thingsWebSocketClient = wsClientResource.getThingsWebsocketClient();
        thingsWebSocketClient.sendProtocolCommand("START-SEND-LIVE-COMMANDS", "START-SEND-LIVE-COMMANDS:ACK").join();
    }

    @Test
    public void postThingCommandWithoutResponseRequiredWithoutExistingPolicy() {
        expectNoSignalAtWebSocketClient();

        final var correlationId = testNameCorrelationId.getCorrelationId();

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "false")
                .body(thing.toJsonString())

                .when()
                .post()

                .then()
                .body(isThingNotCreatableErrorResponse(correlationId));
    }

    @Test
    public void postThingCommandWithResponseRequiredWithoutExistingPolicy() {
        expectNoSignalAtWebSocketClient();

        final var correlationId = testNameCorrelationId.getCorrelationId();

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .body(thing.toJsonString())

                .when()
                .post()

                .then()
                .body(isThingNotCreatableErrorResponse(correlationId));
    }

    @Test
    public void postThingCommandWithResponseRequiredWithExistingPolicy() {
        expectNoSignalAtWebSocketClient();

        final var correlationId = testNameCorrelationId.getCorrelationId();

        final var testSolution = TEST_SOLUTION_RESOURCE.getTestSolution();
        final var defaultNamespace = testSolution.getDefaultNamespace();
        final var policyId = PolicyId.of(defaultNamespace, RandomStringUtils.randomAlphanumeric(7));
        final var readWritePermissions = Permissions.newInstance("READ", "WRITE");
        final var policy = PoliciesModelFactory.newPolicyBuilder()
                .setId(policyId)
                .forLabel("DEFAULT")
                .setSubject(Subject.newInstance(SubjectIdFactory.getSubjectIdForUsername(testSolution.getUsername()),
                        SubjectType.GENERATED))
                .setGrantedPermissions(ResourceKey.newInstance(ThingConstants.ENTITY_TYPE, "/"), readWritePermissions)
                .setGrantedPermissions(ResourceKey.newInstance(PolicyConstants.ENTITY_TYPE, "/"), readWritePermissions)
                .setGrantedPermissions(ResourceKey.newInstance("message", "/"), readWritePermissions)
                .exitLabel()
                .build();
        final var policiesHttpClient = POLICIES_HTTP_CLIENT_RESOURCE.getPoliciesClient();
        policiesHttpClient.putPolicy(policyId, policy, correlationId.withSuffix(".putPolicy"));

        final var thingWithPolicyId = ThingsModelFactory.newThingBuilder(thing)
                .setPolicyId(policyId)
                .build();

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(HttpHeader.RESPONSE_REQUIRED.getName(), "true")
                .body(thingWithPolicyId.toJsonString())

                .when()
                .post()

                .then()
                .body(isThingNotCreatableErrorResponse(correlationId));
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

    private static BodyContainsOnlyExpectedJsonValueMatcher isThingNotCreatableErrorResponse(
            final CorrelationId correlationId
    ) {

        final var dittoHeaders = DittoHeaders.newBuilder().correlationId(correlationId).build();
        final var thingNotCreatableException = ThingNotCreatableException.forLiveChannel(dittoHeaders);
        return new BodyContainsOnlyExpectedJsonValueMatcher(thingNotCreatableException.toJson());
    }

    private void expectNoSignalAtWebSocketClient() {
        final var thingsWebsocketClient = wsClientResource.getThingsWebsocketClient();
        thingsWebsocketClient.onSignal(signal -> {
            final var pattern = "Expected no signal at WebSocket client but received <{0}>.";
            throw new AssertionError(MessageFormat.format(pattern, signal));
        });
    }

}
