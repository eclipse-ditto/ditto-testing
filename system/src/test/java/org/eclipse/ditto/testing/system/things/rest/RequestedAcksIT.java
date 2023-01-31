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
package org.eclipse.ditto.testing.system.things.rest;

import static org.eclipse.ditto.base.model.common.HttpStatus.ACCEPTED;
import static org.eclipse.ditto.base.model.common.HttpStatus.BAD_REQUEST;
import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
import static org.eclipse.ditto.base.model.common.HttpStatus.FAILED_DEPENDENCY;
import static org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT;
import static org.eclipse.ditto.base.model.common.HttpStatus.OK;
import static org.eclipse.ditto.base.model.common.HttpStatus.REQUEST_TIMEOUT;
import static org.eclipse.ditto.json.assertions.DittoJsonAssertions.assertThat;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.acks.AcknowledgementRequest;
import org.eclipse.ditto.base.model.acks.DittoAcknowledgementLabel;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgement;
import org.eclipse.ditto.base.model.signals.acks.AcknowledgementRequestTimeoutException;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonKey;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ServiceEnvironment;
import org.eclipse.ditto.testing.common.Solution;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.ws.ThingsWebsocketClient;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.events.ThingModified;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.restassured.response.Response;

/**
 * Integration Tests for requesting {@link org.eclipse.ditto.base.model.acks.AcknowledgementRequest}s on modifying
 * /things HTTP operations.
 */
public final class RequestedAcksIT extends IntegrationTest {

    private static ThingsWebsocketClient websocketClient1;
    private static ThingsWebsocketClient websocketClient2;
    private static TestingContext testingContext;

    private static AcknowledgementLabel ws1Ack;
    private static AcknowledgementLabel ws2Ack;

    @BeforeClass
    public static void setUpClients() {
        ServiceEnvironment.createSolutionWithRandomUsernameRandomNamespace();
        final Solution solution = ServiceEnvironment.createSolutionWithRandomUsernameRandomNamespace();
        testingContext = TestingContext.withGeneratedMockClient(solution, TEST_CONFIG);
        final AuthClient client1 = testingContext.getOAuthClient();

        ws1Ack = AcknowledgementLabel.of(testingContext.getSolution().getUsername() + ":custom-ack");
        ws2Ack = AcknowledgementLabel.of(testingContext.getSolution().getUsername() + ":another-ack");

        websocketClient1 = newTestWebsocketClient(client1.getAccessToken(),
                Map.of(DittoHeaderDefinition.DECLARED_ACKS.getKey(), String.format("[\"%s\"]", ws1Ack)),
                TestConstants.API_V_2);
        websocketClient2 = newTestWebsocketClient(client1.getAccessToken(),
                Map.of(DittoHeaderDefinition.DECLARED_ACKS.getKey(), String.format("[\"%s\"]", ws2Ack)),
                TestConstants.API_V_2);

        websocketClient1.connect("ThingsWebsocketClient-User1-" + UUID.randomUUID());
        websocketClient2.connect("ThingsWebsocketClient-User1-" + UUID.randomUUID());
    }

    @AfterClass
    public static void tearDownClients() {
        if (websocketClient1 != null) {
            websocketClient1.disconnect();
        }
        if (websocketClient2 != null) {
            websocketClient2.disconnect();
        }
    }

    @Test
    public void createThingRequestingTwinPersistedAcknowledgementAsQueryParam() {
        final Response response = postThing(TestConstants.API_V_2, JsonFactory.newObject())
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .withParam(DittoHeaderDefinition.REQUESTED_ACKS.getKey(), DittoAcknowledgementLabel.TWIN_PERSISTED)
                .expectingHttpStatus(CREATED)
                .fire();

        final String thingId = parseIdFromLocation(response.header("Location"));

        deleteThing(TestConstants.API_V_2, thingId)
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .withParam(DittoHeaderDefinition.REQUESTED_ACKS.getKey(), DittoAcknowledgementLabel.TWIN_PERSISTED)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void createThingRequestingTwinPersistedAcknowledgementAsHeader() {
        final Response response = postThing(TestConstants.API_V_2, JsonFactory.newObject())
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                        DittoAcknowledgementLabel.TWIN_PERSISTED.toString())
                .expectingHttpStatus(CREATED)
                .fire();

        final String thingId = parseIdFromLocation(response.header("Location"));

        deleteThing(TestConstants.API_V_2, thingId)
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                        DittoAcknowledgementLabel.TWIN_PERSISTED.toString())
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void createThingRequestingEmptyAcknowledgements() {
        postThing(TestConstants.API_V_2, JsonFactory.newObject())
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(), "")
                .expectingHttpStatus(CREATED)
                .fire();
    }

    @Test
    public void createThingRequestingEmptyAcknowledgementsAndResponseRequiredFalse() {
        postThing(TestConstants.API_V_2, JsonFactory.newObject())
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(), "")
                .withHeader(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), false)
                .expectingHttpStatus(ACCEPTED)
                .fire();
    }

    @Test
    public void createThingRequestingUnknownAcknowledgement() {
        postThing(TestConstants.API_V_2, JsonFactory.newObject())
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(), "unknown")
                .withHeader(DittoHeaderDefinition.TIMEOUT.getKey(), "3")
                .expectingHttpStatus(REQUEST_TIMEOUT) // resulting in 408 timeout
                .fire();
    }

    @Test
    public void createThingRequestingMultipleAcknowledgementsWhichArePartiallyFulfilled() {
        final String thingId = testingContext.getSolution().getDefaultNamespace() + ":" + UUID.randomUUID();

        final int timeoutInSeconds = 2;
        final String correlationId = "custom-" + UUID.randomUUID();
        final String notFulfilledAckLabel = "not-fulfilled";
        final Response response = putThing(TestConstants.API_V_2, Thing.newBuilder().setId(ThingId.of(thingId)).build(),
                JsonSchemaVersion.V_2)
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .withParam(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId)
                .withParam(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                        DittoAcknowledgementLabel.TWIN_PERSISTED + "," + notFulfilledAckLabel)
                .withParam(DittoHeaderDefinition.TIMEOUT.getKey(), timeoutInSeconds + "s")
                .expectingHttpStatus(FAILED_DEPENDENCY) // resulting in 424 failed dependency
                .fire();

        final JsonObject responseJson = JsonObject.of(response.body().asString());
        final Optional<JsonValue> persisted = responseJson.getValue(DittoAcknowledgementLabel.TWIN_PERSISTED);
        assertThat(persisted).isPresent();
        assertThat(persisted.get()).isObject();
        final JsonObject persistedObj = persisted.get().asObject();
        assertThat(persistedObj)
                .contains(Acknowledgement.JsonFields.STATUS_CODE, JsonValue.of(CREATED.getCode()));
        assertThat(persistedObj).contains(Acknowledgement.JsonFields.PAYLOAD, JsonObject.newBuilder()
                .set(Thing.JsonFields.ID, thingId)
                .set(Thing.JsonFields.POLICY_ID, thingId)
                .build()
        );
        final Optional<JsonObject> persistedHeaders = persistedObj.getValue(Acknowledgement.JsonFields.DITTO_HEADERS);
        assertThat(persistedHeaders).isPresent();
        assertThat(persistedHeaders.get())
                .contains(JsonKey.of(DittoHeaderDefinition.CORRELATION_ID.getKey()), correlationId);
        assertThat(persistedHeaders.get()).containsKey("location");
        assertThat(parseIdFromLocation(persistedHeaders.get().getValue("location").get().asString()))
                .contains(thingId);

        final Optional<JsonValue> notFulfilled = responseJson.getValue(notFulfilledAckLabel);
        assertThat(notFulfilled).isPresent();
        assertThat(notFulfilled.get()).isObject();
        final JsonObject notFulfilledObj = notFulfilled.get().asObject();
        assertThat(notFulfilledObj)
                .contains(Acknowledgement.JsonFields.STATUS_CODE, JsonValue.of(REQUEST_TIMEOUT.getCode()));
        assertThat(notFulfilledObj).contains(Acknowledgement.JsonFields.PAYLOAD,
                AcknowledgementRequestTimeoutException.newBuilder(Duration.ofSeconds(timeoutInSeconds))
                        .build()
                        .toJson()
        );
        final Optional<JsonObject> notFulfilledHeaders =
                notFulfilledObj.getValue(Acknowledgement.JsonFields.DITTO_HEADERS);
        assertThat(notFulfilledHeaders).isPresent();
        assertThat(notFulfilledHeaders.get())
                .contains(JsonKey.of(DittoHeaderDefinition.CORRELATION_ID.getKey()), correlationId);

        deleteThing(TestConstants.API_V_2, thingId)
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .withParam(DittoHeaderDefinition.REQUESTED_ACKS.getKey(), DittoAcknowledgementLabel.TWIN_PERSISTED)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void createThingRequestingPersistedAcknowledgementWithResponseRequiredFalse() {
        postThing(TestConstants.API_V_2, JsonFactory.newObject())
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                        DittoAcknowledgementLabel.TWIN_PERSISTED.toString())
                .withHeader(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), "false")
                .expectingHttpStatus(ACCEPTED) // resulting in 202 accepted
                .fire();
    }

    @Test
    public void createThingRequestingPersistedAcknowledgementWithTimeoutZero() {
        postThing(TestConstants.API_V_2, JsonFactory.newObject())
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                        DittoAcknowledgementLabel.TWIN_PERSISTED.toString())
                .withHeader(DittoHeaderDefinition.TIMEOUT.getKey(), "0")
                .expectingHttpStatus(BAD_REQUEST) // resulting in 400 bad request
                .fire();
    }

    @Test
    @Category(Acceptance.class)
    public void modifyThingRequestingMultipleAcknowledgementsFulfilledViaWs() {
        final String thingId = testingContext.getSolution().getDefaultNamespace() + ":" + UUID.randomUUID();

        final ThingBuilder.FromScratch thingBuilder = Thing.newBuilder().setId(ThingId.of(thingId));
        putThing(TestConstants.API_V_2, thingBuilder.build(), JsonSchemaVersion.V_2)
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .expectingHttpStatus(CREATED)
                .fire();

        final int timeoutInSeconds = 5;
        final String correlationId = "custom-" + UUID.randomUUID();

        final JsonValue customAckPayload = JsonValue.of("I acknowledge formally that this was a success");

        // WS client 1 handles the "custom-ack":
        websocketClient1.startConsumingEvents(event -> {
            if (event instanceof ThingModified) {
                assertThat(event.getDittoHeaders().getAcknowledgementRequests())
                        .contains(AcknowledgementRequest.of(ws1Ack));

                websocketClient1.emit(Acknowledgement.of(ws1Ack,
                        ((ThingModified) event).getEntityId(),
                        OK,
                        DittoHeaders.newBuilder()
                                .correlationId(event.getDittoHeaders().getCorrelationId().get())
                                .contentType("text/plain")
                                .build(),
                        customAckPayload)
                );
            }
        }).join();

        // WS client 2 handles the "another-ack":
        websocketClient2.startConsumingEvents(event -> {
            if (event instanceof ThingModified) {
                assertThat(event.getDittoHeaders().getAcknowledgementRequests())
                        .contains(AcknowledgementRequest.of(ws2Ack));

                websocketClient2.emit(Acknowledgement.of(ws2Ack,
                        ((ThingModified) event).getEntityId(),
                        NO_CONTENT,
                        event.getDittoHeaders())
                );
            }
        }).join();

        final Response updateResponse = putThing(TestConstants.API_V_2, thingBuilder
                        .setAttribute(JsonPointer.of("some"), JsonValue.of(42))
                        .build(), JsonSchemaVersion.V_2)
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .withParam(DittoHeaderDefinition.CORRELATION_ID.getKey(), correlationId)
                .withParam(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                        DittoAcknowledgementLabel.TWIN_PERSISTED + "," + ws1Ack + "," + ws2Ack)
                .withParam(DittoHeaderDefinition.TIMEOUT.getKey(), timeoutInSeconds + "s")
                .expectingHttpStatus(OK) // resulting in 200 OK - as all 3 requested acks were a success
                .fire();

        final JsonObject responseJson = JsonObject.of(updateResponse.body().asString());

        // check "twin-persisted" payload
        final Optional<JsonValue> persisted = responseJson.getValue(DittoAcknowledgementLabel.TWIN_PERSISTED);
        assertThat(persisted).isPresent();
        assertThat(persisted.get()).isObject();
        final JsonObject persistedObj = persisted.get().asObject();
        assertThat(persistedObj).contains(Acknowledgement.JsonFields.STATUS_CODE, JsonValue.of(NO_CONTENT.getCode()));
        final Optional<JsonObject> persistedHeaders = persistedObj.getValue(Acknowledgement.JsonFields.DITTO_HEADERS);
        assertThat(persistedHeaders).isPresent();
        assertThat(persistedHeaders.get())
                .contains(JsonKey.of(DittoHeaderDefinition.CORRELATION_ID.getKey()), correlationId);

        // check "custom-ack" payload
        final Optional<JsonValue> customAck = responseJson.getValue(ws1Ack);
        assertThat(customAck).isPresent();
        assertThat(customAck.get()).isObject();
        final JsonObject customObj = customAck.get().asObject();
        assertThat(customObj)
                .contains(Acknowledgement.JsonFields.STATUS_CODE, JsonValue.of(OK.getCode()));
        assertThat(customObj).contains(Acknowledgement.JsonFields.PAYLOAD, customAckPayload);
        final Optional<JsonObject> customAckHeaders =
                customObj.getValue(Acknowledgement.JsonFields.DITTO_HEADERS);
        assertThat(customAckHeaders).isPresent();
        assertThat(customAckHeaders.get())
                .contains(JsonKey.of(DittoHeaderDefinition.CORRELATION_ID.getKey()), correlationId)
                .contains(JsonKey.of(DittoHeaderDefinition.CONTENT_TYPE.getKey()), "text/plain");

        // check "another-ack" payload
        final Optional<JsonValue> anotherAck = responseJson.getValue(ws2Ack);
        assertThat(anotherAck).isPresent();
        assertThat(anotherAck.get()).isObject();
        final JsonObject anotherObj = anotherAck.get().asObject();
        assertThat(anotherObj).contains(Acknowledgement.JsonFields.STATUS_CODE, JsonValue.of(NO_CONTENT.getCode()));
        assertThat(anotherObj).doesNotContain(Acknowledgement.JsonFields.PAYLOAD);
        final Optional<JsonObject> anotherAckHeaders =
                anotherObj.getValue(Acknowledgement.JsonFields.DITTO_HEADERS);
        assertThat(anotherAckHeaders).isPresent();
        assertThat(anotherAckHeaders.get())
                .contains(JsonKey.of(DittoHeaderDefinition.CORRELATION_ID.getKey()), correlationId);

        deleteThing(TestConstants.API_V_2, thingId)
                .withJWT(testingContext.getOAuthClient().getAccessToken())
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

}
