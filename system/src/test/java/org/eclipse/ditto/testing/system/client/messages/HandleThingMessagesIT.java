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
package org.eclipse.ditto.testing.system.client.messages;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.messages.model.MessageDirection.FROM;
import static org.eclipse.ditto.messages.model.MessageDirection.TO;
import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.http.entity.ContentType;
import org.assertj.core.api.SoftAssertions;
import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.acks.AcknowledgementRequest;
import org.eclipse.ditto.base.model.acks.DittoAcknowledgementLabel;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.common.HttpStatusCodeOutOfRangeException;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.headers.WithDittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgement;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgements;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.management.AcknowledgementsFailedException;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.testing.system.client.util.ThingFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for receiving thing messages with the {@link org.eclipse.ditto.client.DittoClient}.
 */
public final class HandleThingMessagesIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(HandleThingMessagesIT.class);

    private static final int MESSAGE_COUNT = 1;

    private static ThingId thingId1;
    private static ThingId thingId2;

    private static final String CONTENT_TYPE_TEXT = "text/plain";
    private static final String CONTENT_TYPE_IMAGE = TestConstants.CONTENT_TYPE_APPLICATION_RAW_IMAGE;
    private static final String CONTENT_TYPE_XML = TestConstants.CONTENT_TYPE_APPLICATION_XML;
    private static final String CONTENT_TYPE_JSON = "application/json";
    private static final String SUBJECT_1 = "dummySubject";
    private static final String SUBJECT_2 = "anotherDummySubject";
    private static final String TIMEOUT = "30";
    private static final AcknowledgementLabel MY_ACK =
            AcknowledgementLabel.of(serviceEnv.getDefaultAuthUsername() + ":my-ack");

    private DittoClient dittoClient;
    private DittoClient dittoClient2;

    @Before
    public void setUp() throws Exception {
        LOGGER.info("-----> Running Test: {}", name.getMethodName());

        final AuthClient user1 = serviceEnv.getDefaultTestingContext().getOAuthClient();

        dittoClient = newDittoClient(user1);

        thingId1 = ThingId.of(idGenerator().withRandomName());
        final Thing thing1 = ThingFactory.newThing(thingId1);
        thingId2 = ThingId.of(idGenerator().withRandomName());
        final Thing thing2 = ThingFactory.newThing(thingId2);

        dittoClient.twin().create(thing1)
                .thenCompose(thing -> dittoClient.twin().create(thing2))
                .whenComplete((thingAsPersisted, throwable) ->
                {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        dittoClient2 = newDittoClient(user1, Collections.emptyList(), Collections.singleton(MY_ACK));
    }

    @After
    public void tearDown() {
        if (dittoClient != null) {
            try {
                dittoClient.twin().delete(thingId1)
                        .thenCompose(aVoid -> dittoClient.twin().delete(thingId2))
                        .toCompletableFuture()
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (final Exception e) {
                LOGGER.error("Error during Test", e);
            } finally {
                shutdownClient(dittoClient);
                shutdownClient(dittoClient2);
            }
        }
    }

    /**
     * Setup: - one thing - register for messages of the thing, i.e.,
     * integrationClient.twin().forId(thingId1).register... - send MESSAGE_COUNT messages to the first thing Expect:
     * - registration has received MESSAGE_COUNT messages
     */
    @Test
    public void testReceiveThingMessages() throws Exception {
        final CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);

        final String messagePayload = "messagePayload";

        registerForMessages(dittoClient.live().forId(thingId1), SUBJECT_1, latch, message -> {
            assertEquals(SUBJECT_1, message.getSubject());
            final String payload = message.getPayload().map(ByteBuffer::array).map(String::new).orElse("");
            assertEquals(messagePayload, payload);
        });

        dittoClient.live().startConsumption()
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                    sendMessagesOverRest(thingId1, null, SUBJECT_1, CONTENT_TYPE_TEXT, MESSAGE_COUNT,
                            () -> messagePayload);
                });

        awaitLatchTrue(latch);
    }

    /**
     * Setup: - two things - register for all messages of all things, i.e., integrationClient.twin().register... -
     * send MESSAGE_COUNT messages to the first thing - send MESSAGE_COUNT messages to the second thing Expect: -
     * registration has received MESSAGE_COUNT messages for each message registration
     */
    @Test
    public void testReceiveAllThingMessages() throws Exception {
        final CountDownLatch latch1 = new CountDownLatch(MESSAGE_COUNT);
        final CountDownLatch latch2 = new CountDownLatch(MESSAGE_COUNT);

        registerForMessages(dittoClient.live(), SUBJECT_1, latch1);
        registerForMessages(dittoClient.live(), SUBJECT_2, latch2);

        dittoClient.live().startConsumption()
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    sendMessagesOverRest(thingId1, null, SUBJECT_1, CONTENT_TYPE_XML, MESSAGE_COUNT);
                    sendMessagesOverRest(thingId2, null, SUBJECT_2, CONTENT_TYPE_XML, MESSAGE_COUNT);
                });

        awaitLatchTrue(latch1);
        awaitLatchTrue(latch2);
    }

    /**
     * Setup: - two things - register for all messages of the first thing, i.e.,
     * integrationClient.twin().forId(thingId1).register... - register for all messages of the second thing, i.e.,
     * integrationClient.twin().forId(thingId1).register... - send MESSAGE_COUNT messages to the first thing Expect:
     * - first registration has received MESSAGE_COUNT messages - second registration has not received any messages
     */
    @Test
    public void testDoNotReceiveOtherThingsMessages() throws Exception {
        final CountDownLatch latch1 = new CountDownLatch(3 * MESSAGE_COUNT);
        final CountDownLatch latch2 = new CountDownLatch(MESSAGE_COUNT);

        registerForMessages(dittoClient.live().forId(thingId1), SUBJECT_1, latch1);
        registerForMessages(dittoClient.live().forId(thingId2), SUBJECT_2, latch2);

        dittoClient.live().startConsumption()
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    sendMessagesOverRest(thingId1, null, SUBJECT_1, CONTENT_TYPE_IMAGE, MESSAGE_COUNT);
                    sendMessagesOverRest(thingId1, null, SUBJECT_1, CONTENT_TYPE_XML, MESSAGE_COUNT);
                    sendMessagesOverRest(thingId1, null, SUBJECT_1, CONTENT_TYPE_JSON, MESSAGE_COUNT);
                });

        awaitLatchTrue(latch1);
        awaitLatchFalse(latch2);
    }

    @Test
    public void sendMessageWithCustomContentType() throws Exception {
        final CountDownLatch latch1 = new CountDownLatch(MESSAGE_COUNT);

        final String customContentType = TestConstants.CONTENT_TYPE_CUSTOM;
        // payload is not valid UTF-8
        final byte[] payload = new byte[]{
                -1, -1, -1, -1, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', -1, -1, -1
        };

        registerForMessages(dittoClient.live().forId(thingId1), "*", latch1, message -> {
            assertThat(message.getContentType()).contains(customContentType);
            assertThat(message.getPayload().get().array()).isEqualTo(payload);
        });

        dittoClient.live().startConsumption().toCompletableFuture().join();

        postMessage(2, thingId1, TO, SUBJECT_1, customContentType, payload, "0")
                .withHeader(HttpHeader.CONTENT_TYPE, customContentType)
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                /*
                  Use preemptive auth to circumvent restassured issue:
                  https://github.com/rest-assured/rest-assured/issues/507
                */
                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken(), true)
                .fire();

        awaitLatchTrue(latch1);
    }

    @Test
    public void sendMessageWithEmptyRequestedAcksButResponseRequiredTrue() throws Exception {
        final CountDownLatch latch1 = new CountDownLatch(MESSAGE_COUNT);

        final String payload = "Hello";

        registerForMessages(dittoClient.live().forId(thingId1), "*", latch1, message -> {
            assertThat(message.getContentType()).contains("text/plain");
            assertThat(new String(message.getPayload().get().array())).isEqualTo(payload);
            message.reply()
                    .headers(message.getHeaders())
                    .httpStatus(HttpStatus.OK)
                    .payload("Hello you")
                    .send();
        });

        dittoClient.live().startConsumption().toCompletableFuture().join();

        postMessage(2, thingId1, TO, SUBJECT_1, "text/plain", payload, TIMEOUT)
                .withHeader(HttpHeader.CONTENT_TYPE, "text/plain")
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(), "[]")
                .withHeader(DittoHeaderDefinition.RESPONSE_REQUIRED.getKey(), true)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(Matchers.comparesEqualTo("Hello you"))
                /*
                  Use preemptive auth to circumvent restassured issue:
                  https://github.com/rest-assured/rest-assured/issues/507
                */
                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken(), true)
                .fire();

        awaitLatchTrue(latch1);
    }

    @Test
    public void sendMessageWithBinaryPayload() throws Exception {
        final CountDownLatch latch1 = new CountDownLatch(1);

        final URI payloadUri = getClass().getClassLoader().getResource(BINARY_PAYLOAD_RESOURCE).toURI();
        final byte[] binaryBody = Files.readAllBytes(Paths.get(payloadUri));
        final String octetStream = ContentType.APPLICATION_OCTET_STREAM.getMimeType();

        registerForMessages(dittoClient.live().forId(thingId1), "*", latch1, message -> {
            assertThat(message.getContentType()).contains(octetStream);
            assertThat(message.getRawPayload().get().array()).isEqualTo(binaryBody);
        });

        dittoClient.live().startConsumption().toCompletableFuture().join();

        postMessage(2, thingId1, TO, SUBJECT_1, octetStream, binaryBody, "0")
                .withHeader(HttpHeader.CONTENT_TYPE, octetStream)
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                /*
                  Use preemptive auth to circumvent restassured issue:
                  https://github.com/rest-assured/rest-assured/issues/507
                */
                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken(), true)
                .fire();

        awaitLatchTrue(latch1);
    }

    @Test
    @RunIf(DockerEnvironment.class)
    public void testErrorStatusCodes() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final AuthClient solution = serviceEnv.getDefaultTestingContext().getOAuthClient();

        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId).build();
        final Policy policy = newPolicy(PolicyId.of(thingId), List.of(solution),
                List.of(solution));
        putThingWithPolicy(2, thing, policy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        rememberForCleanUp(deleteThing(2, thingId));

        // not authorized = forbidden
        postMessage(2, thingId, FROM, "subject", "text/plain", "", TIMEOUT)
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.FORBIDDEN)
                .fire();

        // nonexistent = not found
        final String nonexistentId = thingId1 + UUID.randomUUID().toString() + ".nonexistent";
        postMessage(2, nonexistentId, FROM, "subject", "text/plain", "", TIMEOUT)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        // invalid thing id = bad request
        final String invalidThingId = "invalid" + UUID.randomUUID();
        postMessage(2, invalidThingId, FROM, "subject", "text/plain", "", TIMEOUT)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();

        // no subject = bad request
        postMessage(2, thingId, FROM, "", "text/plain", "", TIMEOUT)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();

        // no authorization header = unauthorized
        postMessage(2, thingId, FROM, "subject", "text/plain", "", TIMEOUT)
                .expectingHttpStatus(HttpStatus.UNAUTHORIZED)
                .clearAuthentication()
                .fire();
    }

    @Test
    public void testReplyToThingMessages() throws Exception {

        final CountDownLatch latch = new CountDownLatch(2);

        final String requestPayload = "messagePayload";
        final String responsePayload = "42";

        final SoftAssertions softly = new SoftAssertions();

        final String myCustomHeaderKey = "my-custom-header";
        final String myCustomHeaderValue = "foobar";

        dittoClient.live().forId(thingId1).registerForMessage(UUID.randomUUID().toString(), SUBJECT_1, message -> {
            softly.assertThat(message.getSubject()).isEqualTo(SUBJECT_1);
            final String payload = message.getRawPayload().map(ByteBuffer::array).map(String::new)
                    .orElseGet(() -> String.valueOf(message.getPayload().orElse(null)));
            softly.assertThat(payload).isEqualTo(requestPayload);
            message.reply()
                    .headers(DittoHeaders.newBuilder()
                            .putHeader(myCustomHeaderKey, myCustomHeaderValue)
                            .build())
                    .httpStatus(HttpStatus.OK)
                    .payload(responsePayload)
                    .contentType("text/plain")
                    .send();
            latch.countDown();
        });

        CompletableFuture.allOf(
                        dittoClient2.live().startConsumption().toCompletableFuture(),
                        dittoClient.live().startConsumption().toCompletableFuture()
                )
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    dittoClient2.live().forId(thingId1).message()
                            .from()
                            .subject(SUBJECT_1)
                            .payload(requestPayload)
                            .contentType("text/plain")
                            .send(String.class, (response, t) -> {
                                softly.assertThat(response.getPayload())
                                        .describedAs("response payload")
                                        .contains(responsePayload);
                                softly.assertThat(response.getHeaders())
                                        .describedAs("response custom header")
                                        .containsEntry(myCustomHeaderKey, myCustomHeaderValue);
                                latch.countDown();
                            });
                });

        awaitLatchTrue(latch);
        softly.assertAll();
    }

    @Test
    public void testNegativeAcknowledgeToThingMessages() {

        final String requestPayload = "messagePayload";
        final String responsePayload = "42";

        final SoftAssertions softly = new SoftAssertions();

        final String myCustomHeaderKey = "my-custom-header";
        final String myCustomHeaderValue = "foobar";

        final CompletableFuture<Void> acknowledgementSendingFuture = new CompletableFuture<>();
        dittoClient2.live()
                .forId(thingId1)
                .registerForMessage(UUID.randomUUID().toString(), SUBJECT_1, message -> {
                    try {
                        softly.assertThat(message.getSubject()).isEqualTo(SUBJECT_1);
                        final String payload = message.getRawPayload().map(ByteBuffer::array).map(String::new)
                                .orElseGet(() -> String.valueOf(message.getPayload().orElse(null)));
                        softly.assertThat(payload).isEqualTo(requestPayload);
                        message.reply()
                                .headers(DittoHeaders.newBuilder()
                                        .putHeader(myCustomHeaderKey, myCustomHeaderValue)
                                        .build())
                                .httpStatus(HttpStatus.OK)
                                .payload(responsePayload)
                                .contentType("text/plain")
                                .send();
                        message.handleAcknowledgementRequest(MY_ACK,
                                handle -> handle.acknowledge(HttpStatus.IM_A_TEAPOT));
                        acknowledgementSendingFuture.complete(null);
                    } catch (final Throwable error) {
                        acknowledgementSendingFuture.completeExceptionally(error);
                    }
                });

        final CompletableFuture<Void> acknowledgementReceivingFuture = new CompletableFuture<>();
        final CompletableFuture<Void> commandSendingFuture =
                CompletableFuture.allOf(
                        dittoClient.live().startConsumption().toCompletableFuture(),
                        dittoClient2.live().startConsumption().toCompletableFuture()
                ).thenAccept(aVoid -> dittoClient.live().forId(thingId1).message()
                        .from()
                        .subject(SUBJECT_1)
                        .headers(DittoHeaders.newBuilder()
                                .acknowledgementRequest(AcknowledgementRequest.of(MY_ACK),
                                        AcknowledgementRequest.of(DittoAcknowledgementLabel.LIVE_RESPONSE))
                                .build())
                        .payload(requestPayload)
                        .contentType("text/plain")
                        .send(String.class, (message, t) -> {
                            try {
                                assertThat(t).as("Expected throwable to be instance of AcknowledgementsFailedException")
                                        .isInstanceOf(AcknowledgementsFailedException.class);
                                final Acknowledgements acknowledgements =
                                        ((AcknowledgementsFailedException) t).getAcknowledgements();
                                final Optional<Acknowledgement> responseAck =
                                        acknowledgements.getAcknowledgement(DittoAcknowledgementLabel.LIVE_RESPONSE);
                                softly.assertThat(responseAck
                                                .flatMap(Acknowledgement::getEntity))
                                        .map(JsonValue::asString)
                                        .describedAs("response payload")
                                        .contains(responsePayload);
                                softly.assertThat(responseAck
                                                .map(WithDittoHeaders::getDittoHeaders)
                                                .map(headers -> headers.get(myCustomHeaderKey)))
                                        .describedAs("response custom header")
                                        .contains(myCustomHeaderValue);

                                final Optional<Acknowledgement> customAck =
                                        acknowledgements.getAcknowledgement(MY_ACK);
                                softly.assertThat(customAck).isPresent();
                                softly.assertThat(customAck.map(Acknowledgement::getHttpStatus))
                                        .contains(HttpStatus.IM_A_TEAPOT);
                                acknowledgementReceivingFuture.complete(null);
                            } catch (final Throwable error) {
                                acknowledgementReceivingFuture.completeExceptionally(error);
                            }
                        }));

        CompletableFuture.allOf(commandSendingFuture, acknowledgementSendingFuture, acknowledgementReceivingFuture)
                .join();

        softly.assertAll();
    }

    @Test
    public void testAcknowledgeToThingMessages() {

        final String requestPayload = "messagePayload";
        final String responsePayload = "42";

        final SoftAssertions softly = new SoftAssertions();

        final String myCustomHeaderKey = "my-custom-header";
        final String myCustomHeaderValue = "foobar";

        final CompletableFuture<Void> acknowledgementSendingFuture = new CompletableFuture<>();
        dittoClient2.live()
                .forId(thingId1)
                .registerForMessage(UUID.randomUUID().toString(), SUBJECT_1, message -> {
                    try {
                        softly.assertThat(message.getSubject()).isEqualTo(SUBJECT_1);
                        final String payload = message.getRawPayload().map(ByteBuffer::array).map(String::new)
                                .orElseGet(() -> String.valueOf(message.getPayload().orElse(null)));
                        softly.assertThat(payload).isEqualTo(requestPayload);
                        message.reply()
                                .headers(DittoHeaders.newBuilder()
                                        .putHeader(myCustomHeaderKey, myCustomHeaderValue)
                                        .build())
                                .httpStatus(HttpStatus.OK)
                                .payload(responsePayload)
                                .contentType("text/plain")
                                .send();
                        message.handleAcknowledgementRequest(MY_ACK, handle -> handle.acknowledge(HttpStatus.OK));
                        acknowledgementSendingFuture.complete(null);
                    } catch (final Throwable error) {
                        acknowledgementSendingFuture.completeExceptionally(error);
                    }
                });

        final CompletableFuture<Void> acknowledgementReceivingFuture = new CompletableFuture<>();
        final CompletableFuture<Void> commandSendingFuture =
                CompletableFuture.allOf(
                        dittoClient2.live().startConsumption().toCompletableFuture(),
                        dittoClient.live().startConsumption().toCompletableFuture()
                ).thenAccept(aVoid -> dittoClient.live().forId(thingId1).message()
                        .from()
                        .subject(SUBJECT_1)
                        .headers(DittoHeaders.newBuilder()
                                .acknowledgementRequest(AcknowledgementRequest.of(MY_ACK),
                                        AcknowledgementRequest.of(DittoAcknowledgementLabel.LIVE_RESPONSE))
                                .build())
                        .payload(requestPayload)
                        .contentType("text/plain")
                        .send(String.class, (response, t) -> {
                            try {
                                softly.assertThat(response.getPayload())
                                        .describedAs("response payload")
                                        .contains(responsePayload);
                                softly.assertThat(response.getHeaders())
                                        .describedAs("response custom header")
                                        .containsEntry(myCustomHeaderKey, myCustomHeaderValue);
                                acknowledgementReceivingFuture.complete(null);
                            } catch (final Throwable error) {
                                acknowledgementReceivingFuture.completeExceptionally(error);
                            }
                        }));

        CompletableFuture.allOf(commandSendingFuture, acknowledgementSendingFuture, acknowledgementReceivingFuture)
                .join();

        softly.assertAll();
    }

    @Test
    public void sendMessageToThingWithoutPayload() throws Exception {
        final CountDownLatch latch1 = new CountDownLatch(MESSAGE_COUNT);

        final String customContentType = TestConstants.CONTENT_TYPE_APPLICATION_JSON;

        registerForMessages(dittoClient.live().forId(thingId1), "*", latch1, message -> {
            assertThat(message.getContentType()).contains(customContentType);
            assertThat(message.getPayload()).isEmpty();
        });

        dittoClient.live().startConsumption().toCompletableFuture().join();

        postMessage(2, thingId1, TO, SUBJECT_1, customContentType, (String) null, "0")
                .withHeader(HttpHeader.CONTENT_TYPE, customContentType)
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken(), true)
                .fire();

        awaitLatchTrue(latch1);
    }

    @Test
    public void sendMessageFromThingWithoutPayload() throws Exception {
        final CountDownLatch latch1 = new CountDownLatch(MESSAGE_COUNT);

        final String customContentType = TestConstants.CONTENT_TYPE_APPLICATION_JSON;

        registerForMessages(dittoClient.live().forId(thingId1), "*", latch1, message -> {
            assertThat(message.getContentType()).contains(customContentType);
            assertThat(message.getPayload()).isEmpty();
        });

        dittoClient.live().startConsumption().toCompletableFuture().join();

        postMessage(2, thingId1, FROM, SUBJECT_1, customContentType, (String) null, "0")
                .withHeader(HttpHeader.CONTENT_TYPE, customContentType)
                .expectingHttpStatus(HttpStatus.ACCEPTED)
                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken(), true)
                .fire();

        awaitLatchTrue(latch1);
    }

    @Test
    public void sendMessageWithContentTypeOctetStream() throws Exception {

        final CountDownLatch liveMessagesReceived = new CountDownLatch(MESSAGE_COUNT);

        final JsonObject payload = JsonFactory.newObjectBuilder()
                .set("value", "testValue")
                .build();

        final JsonObject responsePayload = JsonFactory.newObjectBuilder()
                .set("description", "Invalid test parameter.")
                .set("error", "messages:parameter.invalid")
                .set("message", "Invalid test parameter.")
                .set("status", 400)
                .build();

        registerForMessages(dittoClient.live().forId(thingId1), "*", liveMessagesReceived, message -> {

            message.reply()
                    .headers(message.getHeaders())
                    .correlationId(message.getCorrelationId().get())
                    .httpStatus(HttpStatus.BAD_REQUEST)
                    .payload(responsePayload.toString())
                    .contentType(TestConstants.CONTENT_TYPE_APPLICATION_OCTET_STREAM)
                    .send();
        });

        dittoClient.live().startConsumption().toCompletableFuture().get();

        postMessage(2, thingId1, TO, SUBJECT_1, ContentType.APPLICATION_JSON.toString(), payload.toString(), TIMEOUT)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(Matchers.comparesEqualTo(responsePayload.toString()))
                .expectingHeader(HttpHeader.CONTENT_TYPE.getName(), TestConstants.CONTENT_TYPE_APPLICATION_OCTET_STREAM)
                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken(), true)
                .fire();

        awaitLatchTrue(liveMessagesReceived);
    }

    private static final HttpStatus NON_STANDARD_HTTP_STATUS;

    static {
        try {
            NON_STANDARD_HTTP_STATUS = HttpStatus.getInstance(443);
        } catch (HttpStatusCodeOutOfRangeException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sendResponseWithNonStandardHttpStatus() throws ExecutionException, InterruptedException {
        final CountDownLatch liveMessagesReceived = new CountDownLatch(MESSAGE_COUNT);

        final String payload = "request";
        final String responsePayload = "response";

        registerForMessages(dittoClient.live().forId(thingId1), "*", liveMessagesReceived, message ->
                message.reply()
                        .headers(message.getHeaders())
                        .correlationId(message.getCorrelationId().get())
                        .httpStatus(NON_STANDARD_HTTP_STATUS)
                        .payload(responsePayload)
                        .contentType(TestConstants.CONTENT_TYPE_TEXT_PLAIN)
                        .send());

        dittoClient.live().startConsumption().toCompletableFuture().get();

        postMessage(2, thingId1, TO, SUBJECT_1, TestConstants.CONTENT_TYPE_TEXT_PLAIN, payload, "20")
                .expectingHttpStatus(NON_STANDARD_HTTP_STATUS)
                .expectingBody(Matchers.comparesEqualTo(responsePayload))
                .expectingHeader(HttpHeader.CONTENT_TYPE.getName(), TestConstants.CONTENT_TYPE_TEXT_PLAIN)
                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken(), true)
                .fire();

        awaitLatchTrue(liveMessagesReceived);
    }

}
