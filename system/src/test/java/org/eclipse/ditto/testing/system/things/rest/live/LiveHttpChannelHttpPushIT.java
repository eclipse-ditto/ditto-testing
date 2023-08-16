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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.http.HttpStatus;
import org.assertj.core.api.JUnitSoftAssertions;
import org.eclipse.ditto.base.model.common.DittoConstants;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.translator.HeaderTranslator;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.commands.CommandResponse;
import org.eclipse.ditto.connectivity.model.LogCategory;
import org.eclipse.ditto.connectivity.model.LogEntry;
import org.eclipse.ditto.connectivity.model.LogLevel;
import org.eclipse.ditto.connectivity.model.LogType;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonParseException;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
import org.eclipse.ditto.protocol.adapter.ProtocolAdapter;
import org.eclipse.ditto.testing.common.ConnectionsHttpClientResource;
import org.eclipse.ditto.testing.common.TestSolutionResource;
import org.eclipse.ditto.testing.common.ThingJsonProducer;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.config.ThingsBaseUriResource;
import org.eclipse.ditto.testing.common.connectivity.ConnectionResource;
import org.eclipse.ditto.testing.common.connectivity.http.HttpPushConfig;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.correlationid.TestNameCorrelationId;
import org.eclipse.ditto.testing.common.http.HttpTestServer;
import org.eclipse.ditto.testing.common.http.HttpTestServerResource;
import org.eclipse.ditto.testing.common.matcher.BodyContainsOnlyExpectedJsonValueMatcher;
import org.eclipse.ditto.testing.common.policies.PoliciesHttpClient;
import org.eclipse.ditto.testing.common.policies.PoliciesHttpClientResource;
import org.eclipse.ditto.testing.common.policies.PolicyWithConnectionSubjectResource;
import org.eclipse.ditto.testing.common.things.ThingsHttpClientResource;
import org.eclipse.ditto.testing.system.things.rest.ThingResource;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttribute;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttributeResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrievePolicyId;
import org.eclipse.ditto.things.model.signals.commands.query.RetrievePolicyIdResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThingResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.pekko.http.javadsl.model.ContentTypes;
import org.apache.pekko.http.javadsl.model.HttpEntities;
import org.apache.pekko.http.javadsl.model.HttpEntity;
import org.apache.pekko.http.javadsl.model.HttpHeader;
import org.apache.pekko.http.javadsl.model.HttpRequest;
import org.apache.pekko.http.javadsl.model.HttpResponse;
import org.apache.pekko.http.javadsl.model.ResponseEntity;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;

/**
 * This test uses a HTTP Push connection to check the behaviour of live commands via HTTP.
 */
public final class LiveHttpChannelHttpPushIT {

    private static final String CONNECTION_NAME = LiveHttpChannelHttpPushIT.class.getSimpleName();
    private static final TestConfig TEST_CONFIG = TestConfig.getInstance();
    private static final HttpPushConfig HTTP_PUSH_CONFIG = HttpPushConfig.of(TEST_CONFIG);
    private static final String SIGNAL_TARGET_TOPIC = "POST:/target/address";

    // This protocol adapter filters nothing.
    private static final ProtocolAdapter PROTOCOL_ADAPTER = DittoProtocolAdapter.of(HeaderTranslator.empty());

    @ClassRule(order = 0)
    public static final HttpTestServerResource HTTP_TEST_SERVER_RESOURCE = HttpTestServerResource.newInstance();

    @ClassRule(order = 0)
    public static final TestSolutionResource TEST_SOLUTION_RESOURCE = TestSolutionResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 1)
    public static final ConnectionsHttpClientResource CONNECTIONS_CLIENT_RESOURCE =
            ConnectionsHttpClientResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 1)
    public static final PoliciesHttpClientResource POLICIES_HTTP_CLIENT_RESOURCE =
            PoliciesHttpClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @ClassRule(order = 2)
    public static final PolicyWithConnectionSubjectResource POLICY_WITH_CONNECTION_SUBJECT_RESOURCE =
            PolicyWithConnectionSubjectResource.newInstance(TEST_SOLUTION_RESOURCE,
                    POLICIES_HTTP_CLIENT_RESOURCE, CONNECTION_NAME);
    @ClassRule(order = 1)
    public static final ThingsHttpClientResource THINGS_HTTP_CLIENT_RESOURCE =
            ThingsHttpClientResource.newInstance(TEST_CONFIG, TEST_SOLUTION_RESOURCE);

    @ClassRule
    public static final ThingsBaseUriResource THINGS_BASE_URI_RESOURCE = ThingsBaseUriResource.newInstance(TEST_CONFIG);

    @ClassRule(order = 2)
    public static final ConnectionResource HTTP_PUSH_SOLUTION_CONNECTION_RESOURCE =
            ConnectionResource.newInstance(TEST_CONFIG.getTestEnvironment(),
                    CONNECTIONS_CLIENT_RESOURCE,
                    () -> HttpPushConnectionFactory.getHttpPushConnection(HTTP_PUSH_CONFIG.getHostName(),
                            HTTP_TEST_SERVER_RESOURCE.getHttpTestServerPort(),
                            TEST_SOLUTION_RESOURCE.getTestUsername(),
                            CONNECTION_NAME,
                            SIGNAL_TARGET_TOPIC));

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
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();

    private static HttpTestServer httpTestServer;
    private static PoliciesHttpClient policiesHttpClient;

    @BeforeClass
    public static void beforeClass() throws InterruptedException {
        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();
        httpTestServer = HTTP_TEST_SERVER_RESOURCE.getHttpTestServer();
        policiesHttpClient = POLICIES_HTTP_CLIENT_RESOURCE.getPoliciesClient();
    }

    @Test
    public void sendLiveModifyAttributeViaHttpToHttpPushConnection() {
        final var attributePath = "manufacturer";
        final var correlationId = testNameCorrelationId.getCorrelationId(".putAttribute");

        httpTestServer.setResponseFunction(request -> {
            final var signal = getSignalFromRequest(request);
            softly.assertThat(signal).isInstanceOf(ModifyAttribute.class);

            final var modifyAttributeResponse = ModifyAttributeResponse.modified(thingResource.getThingId(),
                    JsonPointer.of(attributePath),
                    signal.getDittoHeaders());

            return CompletableFuture.completedFuture(HttpResponse.create()
                    .withStatus(HttpStatus.SC_OK)
                    .addHeader(getCorrelationIdHeader(correlationId))
                    .withEntity(getAsDittoProtocolMessageHttpEntity(modifyAttributeResponse)));
        });

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "true")
                .body("\"Bosch IO\"")

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingResource.getThingId().toString(), attributePath)

                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    @Test
    public void sendLiveRetrievePolicyIdViaHttpToHttpPushConnection() {
        final var correlationId = testNameCorrelationId.getCorrelationId(".getPolicyId");
        final var policyId = thingResource.getThing().getPolicyId().orElseThrow();

        httpTestServer.setResponseFunction(request -> {
            final var signal = getSignalFromRequest(request);
            softly.assertThat(signal).isInstanceOf(RetrievePolicyId.class);

            final var retrievePolicyIdResponse = RetrievePolicyIdResponse.of(thingResource.getThingId(),
                    policyId,
                    signal.getDittoHeaders());

            return CompletableFuture.completedFuture(HttpResponse.create()
                    .withStatus(HttpStatus.SC_OK)
                    .addHeader(getCorrelationIdHeader(correlationId))
                    .withEntity(getAsDittoProtocolMessageHttpEntity(retrievePolicyIdResponse)));
        });

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(correlationId))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "true")

                .when()
                .get("/{thingId}/policyId", thingResource.getThingId().toString())

                .then()
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(JsonValue.of(policyId)))
                .statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void sendLiveRetrieveThingViaHttpToHttpPushConnectionWithRestrictedPermission() {
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        httpTestServer.setResponseFunction(request -> {
            final var signal = getSignalFromRequest(request);
            softly.assertThat(signal).isInstanceOf(RetrieveThing.class);

            final var retrieveThingResponse = RetrieveThingResponse.of(thingResource.getThingId(),
                    thingResource.getThing().toJson(),
                    signal.getDittoHeaders());

            return CompletableFuture.completedFuture(HttpResponse.create()
                    .withStatus(HttpStatus.SC_OK)
                    .addHeader(getCorrelationIdHeader(baseCorrelationId))
                    .withEntity(getAsDittoProtocolMessageHttpEntity(retrieveThingResponse)));
        });

        final var policyId = PolicyId.of(POLICY_WITH_CONNECTION_SUBJECT_RESOURCE.getPolicyId());
        final var policy = policiesHttpClient.getPolicy(policyId, baseCorrelationId.withSuffix(".getPolicy"));
        final var adjustedPolicy = PoliciesModelFactory.newPolicyBuilder(policy.orElseThrow())
                .setRevokedPermissionsFor("DEFAULT", "thing", "/attributes/model", "READ")
                .setRevokedPermissionsFor("DEFAULT", "thing", "/features/Vehicle/properties/status", "READ")
                .setRevokedPermissionsFor("DEFAULT",
                        "thing",
                        "/features/EnvironmentScanner/properties/location",
                        "READ")
                .build();
        policiesHttpClient.putPolicy(policyId, adjustedPolicy, baseCorrelationId.withSuffix(".putAdjustedPolicy"));

        final var expectedThing = thingResource.getThing().toBuilder()
                .removeAttribute(JsonPointer.of("model"))
                .removeFeatureProperty("Vehicle", JsonPointer.of("status"))
                .removeFeatureProperty("EnvironmentScanner", JsonPointer.of("location"))
                .build();

        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(testNameCorrelationId.getCorrelationId()))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.name(), "true")

                .when()
                .get("/{thingId}", thingResource.getThingId().toString())

                .then()
                .body(new BodyContainsOnlyExpectedJsonValueMatcher(expectedThing.toJson()))
                .statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void sendInvalidResponseFromDevice() {
        final var baseCorrelationId = testNameCorrelationId.getCorrelationId();

        // Enable connection logs.
        final var solutionsHttpClient = CONNECTIONS_CLIENT_RESOURCE.getConnectionsClient();
        solutionsHttpClient.postConnectionCommand(HTTP_PUSH_SOLUTION_CONNECTION_RESOURCE.getConnectionId(),
                "connectivity.commands:enableConnectionLogs",
                baseCorrelationId.withSuffix(".enableConnectionLogs"));

        // Set function that sends an invalid response.
        httpTestServer.setResponseFunction(httpRequest -> {
            final var command = getSignalFromRequest(httpRequest);
            final var retrieveThingResponse = RetrieveThingResponse.of(thingResource.getThingId(),
                    thingResource.getThing().toJson(),
                    command.getDittoHeaders());

            return CompletableFuture.completedFuture(HttpResponse.create()
                    .withStatus(HttpStatus.SC_OK)
                    .addHeader(getCorrelationIdHeader(baseCorrelationId))
                    .withEntity(getAsDittoProtocolMessageHttpEntity(retrieveThingResponse)));
        });

        // Send HTTP live request.
        RestAssured.given(getBasicThingsRequestSpecWithLiveQuery(baseCorrelationId))
                .header(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "true")
                .header(DittoHeaderDefinition.TIMEOUT.getKey(), "5s")
                .body(JsonValue.of("Bosch IO").toString())

                .when()
                .put("/{thingId}/attributes/{attributePath}", thingResource.getThingId().toString(), "manufacturer")

                .then()
                .statusCode(HttpStatus.SC_REQUEST_TIMEOUT)
                .body(containsString("Try increasing the command timeout."));

        // Search in connection log for expected failure entry.
        final var connectionLogs = solutionsHttpClient.getConnectionLogs(
                HTTP_PUSH_SOLUTION_CONNECTION_RESOURCE.getConnectionId(),
                baseCorrelationId.withSuffix(".getConnectionLogs"));

        assertThat(LogEntriesFromJsonConverter.getLogEntries(connectionLogs))
                .filteredOn(LogEntry::getCorrelationId, baseCorrelationId.toString())
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
                .build();
    }

    private static Signal<?> getSignalFromRequest(final HttpRequest message) {
        final var jsonifiableAdaptable = getJsonifiableAdaptable(getAsUtf8String(message.entity()));
        return PROTOCOL_ADAPTER.fromAdaptable(ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build());
    }

    private static String getAsUtf8String(final HttpEntity httpEntity) {
        final var timeout = 5000L;
        return httpEntity.toStrict(timeout, httpTestServer.getMat())
                .toCompletableFuture()
                .join()
                .getData()
                .utf8String();
    }

    private static JsonifiableAdaptable getJsonifiableAdaptable(final String string) {
        try {
            final var jsonValue = JsonFactory.readFrom(string);
            if (jsonValue.isObject()) {
                return ProtocolFactory.jsonifiableAdaptableFromJson(jsonValue.asObject());
            } else {
                throw new JsonParseException("The response message was not a JSON object as required: " + string);
            }
        } catch (final JsonParseException e) {
            Assert.fail("Got unknown non-JSON response message: " + string);
            throw e;
        }
    }

    private static ResponseEntity getAsDittoProtocolMessageHttpEntity(final CommandResponse<?> commandResponse) {
        return HttpEntities.create(ContentTypes.parse(DittoConstants.DITTO_PROTOCOL_CONTENT_TYPE),
                getAsDittoProtocolMessageUtf8Bytes(commandResponse));
    }

    private static byte[] getAsDittoProtocolMessageUtf8Bytes(final Signal<?> signal) {
        final var dittoProtocolStringMessage = getAsDittoProtocolStringMessage(signal);
        return dittoProtocolStringMessage.getBytes(StandardCharsets.UTF_8);
    }

    private static String getAsDittoProtocolStringMessage(final Signal<?> signal) {
        final var adaptable = PROTOCOL_ADAPTER.toAdaptable(signal);
        final var jsonifiableAdaptable = ProtocolFactory.wrapAsJsonifiableAdaptable(adaptable);
        return jsonifiableAdaptable.toJsonString();
    }

    private static HttpHeader getCorrelationIdHeader(final CorrelationId correlationId) {
        return HttpHeader.parse(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId.toString());
    }

}
