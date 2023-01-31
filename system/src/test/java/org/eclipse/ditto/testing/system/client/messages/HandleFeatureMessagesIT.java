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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.apache.http.entity.ContentType;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.live.messages.RepliableMessage;
import org.eclipse.ditto.client.options.Options;
import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for receiving feature messages with the {@link org.eclipse.ditto.client.DittoClient}.
 */
public final class HandleFeatureMessagesIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(HandleFeatureMessagesIT.class);

    private static final int MESSAGE_COUNT = 1;

    private static final String CONTENT_TYPE_IMAGE = TestConstants.CONTENT_TYPE_APPLICATION_RAW_IMAGE;
    private static final String FEATURE_ID_1 = "feature1";
    private static final String FEATURE_ID_2 = "feature2";
    private static final String SUBJECT = "dummySubject";
    private static final String TIMEOUT = "30";

    private AuthClient user1;
    private AuthClient user2;

    private DittoClient dittoClient;

    @Before
    public void setUp() {
        LOGGER.info("-----> Running Test: {}", name.getMethodName());

        user1 = serviceEnv.getDefaultTestingContext().getOAuthClient();
        user2 = serviceEnv.getTestingContext2().getOAuthClient();
        dittoClient = newDittoClient(user1);
    }

    @After
    public void tearDown() {
        shutdownClient(dittoClient);
    }
    
    @Test
    public void testReceiveFeatureMessages() throws InterruptedException, TimeoutException, ExecutionException {
        final CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setFeature(FEATURE_ID_1)
                .build();

        registerForMessages(dittoClient.live().forId(thingId).forFeature(FEATURE_ID_1), SUBJECT, latch);

        dittoClient.twin().create(thing)
                .thenCompose(thingAsPersisted -> dittoClient.live().startConsumption())
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    sendMessagesOverRest(thingId, FEATURE_ID_1, SUBJECT, CONTENT_TYPE_IMAGE, MESSAGE_COUNT);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertTrue("Did not receive message within timeout.", latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS));
    }
    
    @Test
    public void testFeatureDoesNotReceiveMessagesOfOtherFeatures()
            throws InterruptedException, TimeoutException, ExecutionException {
        final CountDownLatch latch1 = new CountDownLatch(MESSAGE_COUNT);
        final CountDownLatch latch2 = new CountDownLatch(MESSAGE_COUNT);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setFeature(FEATURE_ID_1)
                .setFeature(FEATURE_ID_2).build();

        registerForMessages(dittoClient.live().forId(thingId).forFeature(FEATURE_ID_1), SUBJECT, latch1);
        registerForMessages(dittoClient.live().forId(thingId).forFeature(FEATURE_ID_2), SUBJECT, latch2);

        dittoClient.twin().create(thing)
                .thenCompose(thingAsPersisted -> dittoClient.live().startConsumption())
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    sendMessagesOverRest(thingId, FEATURE_ID_1, SUBJECT, CONTENT_TYPE_IMAGE, MESSAGE_COUNT);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertTrue("Did not receive message within timeout.", latch1.await(LATCH_TIMEOUT, TimeUnit.SECONDS));
        assertEquals(MESSAGE_COUNT, latch2.getCount());
    }
    
    @Test
    public void testGlobalHandlerReceivesFeatureMessagesSentForFeature()
            throws InterruptedException, TimeoutException, ExecutionException {
        final CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setFeature(FEATURE_ID_1)
                .setFeature(FEATURE_ID_2).build();

        registerForMessages(dittoClient.live(), SUBJECT, latch);

        dittoClient.twin().create(thing)
                .thenCompose(thingAsPersisted -> dittoClient.live().startConsumption())
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    sendMessagesOverRest(thingId, FEATURE_ID_1, SUBJECT, CONTENT_TYPE_IMAGE, MESSAGE_COUNT);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertTrue("Did not receive message within timeout.", latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    public void testSeveralHandlersReceiveFeatureMessagesSentForFeature()
            throws InterruptedException, TimeoutException, ExecutionException {
        final int invalidLatchCount = 4;
        final CountDownLatch latchValid = new CountDownLatch(6);
        final CountDownLatch latchInvalid = new CountDownLatch(invalidLatchCount);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), List.of(user1, user2), List.of(user1));

        /*
         * All of the following 6 registrations should get the message:
         */
        // global registration for all messages:
        dittoClient.live().registerForMessage(UUID.randomUUID().toString(), "*",
                getMessageConsumer(latchValid, thingId, "global + *")); // 1
        // global registration for the special message subject:
        dittoClient.live().registerForMessage(UUID.randomUUID().toString(), SUBJECT,
                getMessageConsumer(latchValid, thingId, "global + " + SUBJECT)); // 2
        // registration on thing level for all messages:
        dittoClient.live().forId(thingId).registerForMessage(UUID.randomUUID().toString(), "*",
                getMessageConsumer(latchValid, thingId, "thingId + *")); // 3
        // registration on thing level for the special message subject:
        dittoClient.live().forId(thingId).registerForMessage(UUID.randomUUID().toString(), SUBJECT,
                getMessageConsumer(latchValid, thingId, "thingId + " + SUBJECT)); // 4
        // registration on feature level for all messages:
        dittoClient.live().forFeature(thingId, FEATURE_ID_1).registerForMessage(UUID.randomUUID().toString(), "*",
                getMessageConsumer(latchValid, thingId, "thingId + featureId + *")); // 5
        // registration on feature level for the special message subject:
        dittoClient.live().forFeature(thingId, FEATURE_ID_1).registerForMessage(UUID.randomUUID().toString(), SUBJECT,
                getMessageConsumer(latchValid, thingId, "thingId + featureId + " + SUBJECT)); // 6

        final Consumer<RepliableMessage<?, String>> invalidMessageConsumer = rawMessage -> {
            LOGGER.debug("Message received: {}", rawMessage);
            latchInvalid.countDown();
        };

        /*
         * All of the following 4 registrations should NOT get the message:
         */
        // registration on thing level for a different thingId for all messages:
        dittoClient.live().forId(ThingId.of("some:other.thing")).registerForMessage(UUID.randomUUID().toString(),
                "*", invalidMessageConsumer);
        // registration on thing level for the special message subject:
        dittoClient.live()
                .forId(ThingId.of("some:other.thing"))
                .registerForMessage(UUID.randomUUID().toString(), SUBJECT,
                        invalidMessageConsumer);
        // registration on feature level with a different FEATURE_ID for all messages:
        dittoClient.live().forFeature(thingId, "different").registerForMessage(UUID.randomUUID().toString(),
                "*", invalidMessageConsumer);
        // registration on feature level for the special message subject:
        dittoClient.live().forFeature(thingId, "different").registerForMessage(UUID.randomUUID().toString(),
                SUBJECT, invalidMessageConsumer);

        dittoClient.twin().create(thing, policy)
                .thenCompose(thingAsPersisted -> {
                    rememberForCleanUp(deleteThing(2, thingId));
                    return dittoClient.live().startConsumption();
                })
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    sendMessagesOverRest(thingId, FEATURE_ID_1, SUBJECT, CONTENT_TYPE_IMAGE, MESSAGE_COUNT);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertTrue("Did not receive messages within timeout", latchValid.await(LATCH_TIMEOUT, TimeUnit.SECONDS));
        assertFalse("Did receive messages which was not expected", latchInvalid.await(LATCH_TIMEOUT, TimeUnit.SECONDS));
        assertEquals("InvalidLatch count should be " + invalidLatchCount, invalidLatchCount, latchInvalid.getCount());
    }

    @Test
    @RunIf(DockerEnvironment.class)
    public void testErrorStatusCodes() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final Thing thing = newThing(thingId);
        final Policy policy =
                newPolicy(PolicyId.of(thingId), List.of(user1, user2), List.of(user1));
        putThingWithPolicy(2, thing, policy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        rememberForCleanUp(deleteThing(2, thingId));

        // not authorized = forbidden
        postMessage(2, thingId, FEATURE_ID_1, FROM, "subject", "text/plain",
                "test message", TIMEOUT)
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.FORBIDDEN)
                .fire();

        // no WRITE access = forbidden
        postMessage(2, thingId, FEATURE_ID_1, FROM, "subject", "text/plain",
                "test message", TIMEOUT)
                .withJWT(serviceEnv.getTestingContext3().getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.FORBIDDEN)
                .fire();

        // nonexistent thing = not found
        final String nonexistentId = idGenerator().withSuffixedRandomName("nonexistent");
        postMessage(2, nonexistentId, FEATURE_ID_1, FROM, "subject", "text/plain",
                "test message", TIMEOUT)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        // invalid thing id = bad request
        final String invalidThingId = "invalid" + UUID.randomUUID();
        postMessage(2, invalidThingId, FEATURE_ID_1, FROM, "subject", "text/plain",
                "test message", TIMEOUT)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();

        // no subject = bad request
        postMessage(2, thingId, FEATURE_ID_1, FROM, "", "text/plain",
                "test message", TIMEOUT)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();

        // no authorization header = unauthorized
        postMessage(2, thingId, FEATURE_ID_1, FROM, "subject", "text/plain",
                "test message", TIMEOUT)
                .expectingHttpStatus(HttpStatus.UNAUTHORIZED)
                .clearAuthentication()
                .fire();
    }

    @Test
    public void sendMessagesFromNonexistentFeatures() {
        // nonexistent feature CAN handle messages?!
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), List.of(user1, user2), List.of(user1));
        final String nonexistentFeatureId = "nonexistentFeature";
        putThingWithPolicy(2, thing, policy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        rememberForCleanUp(deleteThing(2, thingId));
        dittoClient.live()
                .forId(thingId)
                .forFeature(nonexistentFeatureId)
                .registerForMessage(UUID.randomUUID().toString(), "*", repliableMessage ->
                        repliableMessage.reply()
                                .httpStatus(HttpStatus.IM_A_TEAPOT)
                                .send());
        dittoClient.live().startConsumption().toCompletableFuture().join();
        postMessage(2, thingId, nonexistentFeatureId, FROM, nonexistentFeatureId, "text/plain", "test message", TIMEOUT)
                .withParam("timeout", TIMEOUT)
                .expectingHttpStatus(HttpStatus.IM_A_TEAPOT)
                .fire();
    }

    @Test
    public void sendMessageWithCustomContentType() throws Exception {
        final CountDownLatch latch1 = new CountDownLatch(MESSAGE_COUNT);

        final String customContentType = TestConstants.CONTENT_TYPE_CUSTOM;
        // payload is not valid UTF-8
        final byte[] payload = new byte[]{
                -1, -1, -1, -1, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', -1, -1, -1
        };

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final String subject = "subject";
        final Thing thing = newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), List.of(user1, user2), List.of(user1));
        putThingWithPolicy(2, thing, policy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        rememberForCleanUp(deleteThing(2, thingId));

        registerForMessages(dittoClient.live().forId(thingId).forFeature(FEATURE_ID_1), "*", latch1, message -> {
            assertThat(message.getContentType()).contains(customContentType);
            assertThat(message.getPayload().get().array()).isEqualTo(payload);
        });

        dittoClient.live().startConsumption().toCompletableFuture().join();

        postMessage(2, thingId, FEATURE_ID_1, TO, subject, customContentType, payload, "0")
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
    public void sendMessageWithBinaryPayload() throws Exception {
        final CountDownLatch latch1 = new CountDownLatch(1);

        final URI payloadUri = getClass().getClassLoader().getResource(BINARY_PAYLOAD_RESOURCE).toURI();
        final byte[] binaryBody = Files.readAllBytes(Paths.get(payloadUri));
        final String octetStream = ContentType.APPLICATION_OCTET_STREAM.getMimeType();

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final String subject = "subject";
        final Thing thing = newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), List.of(user1, user2), List.of(user1));
        putThingWithPolicy(2, thing, policy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        rememberForCleanUp(deleteThing(2, thingId));

        registerForMessages(dittoClient.live().forId(thingId).forFeature(FEATURE_ID_1), "*", latch1, message -> {
            assertThat(message.getContentType()).contains(octetStream);
            assertThat(message.getRawPayload().get().array()).isEqualTo(binaryBody);
        });

        dittoClient.live().startConsumption().toCompletableFuture().join();

        // run request using apache httpclient because restassured can't encode binary entities as repeatable
        postMessage(2, thingId, FEATURE_ID_1, TO, subject, octetStream, binaryBody, "0")
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
    public void testReceiveFeatureMessageWithEnrichedField()
            throws InterruptedException, TimeoutException, ExecutionException {
        final CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(JsonPointer.of("location"), JsonValue.of("kitchen"))
                .setFeature(FEATURE_ID_1)
                .build();

        registerForMessages(dittoClient.live().forId(thingId).forFeature(FEATURE_ID_1), SUBJECT, latch, message -> {
            assertThat(message.getContentType()).contains(CONTENT_TYPE_IMAGE);
            assertThat(message.getExtra()).isPresent();
            assertThat(message.getExtra().get().getValue("attributes/location")).contains(JsonValue.of("kitchen"));
        });

        dittoClient.twin().create(thing)
                .thenCompose(thingAsPersisted -> dittoClient.live().startConsumption(Options.Consumption.extraFields(
                        JsonFieldSelector.newInstance("attributes/location"))))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    sendMessagesOverRest(thingId, FEATURE_ID_1, SUBJECT, CONTENT_TYPE_IMAGE, MESSAGE_COUNT);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertTrue("Did not receive message within timeout.", latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    public void testSendMessageToFeatureWithoutContent()
            throws InterruptedException, TimeoutException, ExecutionException {
        final CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setFeature(FEATURE_ID_1)
                .build();

        registerForMessages(dittoClient.live().forId(thingId).forFeature(FEATURE_ID_1), SUBJECT, latch, message -> {
            assertThat(message.getPayload()).isEmpty();
        });

        dittoClient.twin().create(thing)
                .thenCompose(thingAsPersisted -> dittoClient.live().startConsumption())
                .whenComplete((aVoid, throwable) -> {
                    System.out.println("going to send shit");
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                    postMessage(2, thingId, FEATURE_ID_1, TO, SUBJECT, CONTENT_TYPE_IMAGE,
                            (String) null, "0")
                            .withHeader(HttpHeader.CONTENT_TYPE, CONTENT_TYPE_IMAGE)
                            .expectingHttpStatus(HttpStatus.ACCEPTED)
                            .fire();
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertTrue("Did not receive message within timeout.", latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    public void testSendMessageFromFeatureWithoutContent()
            throws InterruptedException, TimeoutException, ExecutionException {
        final CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setFeature(FEATURE_ID_1)
                .build();

        registerForMessages(dittoClient.live().forId(thingId).forFeature(FEATURE_ID_1), SUBJECT, latch, message -> {
            assertThat(message.getPayload()).isEmpty();
        });

        dittoClient.twin().create(thing)
                .thenCompose(thingAsPersisted -> dittoClient.live().startConsumption())
                .whenComplete((aVoid, throwable) -> {
                    System.out.println("going to send shit");
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                    postMessage(2, thingId, FEATURE_ID_1, FROM, SUBJECT, CONTENT_TYPE_IMAGE, (String) null, "0")
                            .withHeader(HttpHeader.CONTENT_TYPE, CONTENT_TYPE_IMAGE)
                            .expectingHttpStatus(HttpStatus.ACCEPTED)
                            .fire();
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertTrue("Did not receive message within timeout.", latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS));
    }

    @Test
    public void testFilterMessagesSubscriptionUsingRqlTopic()
            throws InterruptedException, TimeoutException, ExecutionException {
        final CountDownLatch latch = new CountDownLatch(3);
        final CountDownLatch negativeLatch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setFeature(FEATURE_ID_1)
                .setFeature(FEATURE_ID_2).build();

        // we expect to receive messages for the following 3 subjects:
        registerForMessages(dittoClient.live(), SUBJECT, latch);
        registerForMessages(dittoClient.live(), "positive-vibes", latch);
        registerForMessages(dittoClient.live(), "positiveNess", latch);

        // we expect to NOT receive messages for the following 2 subjects:
        registerForMessages(dittoClient.live(), "non-matching-subject", negativeLatch);
        registerForMessages(dittoClient.live(), "what-is-this", negativeLatch);

        dittoClient.twin().create(thing)
                .thenCompose(thingAsPersisted -> dittoClient.live().startConsumption(
                        Options.Consumption
                                .filter("or(eq(topic:subject,'" + SUBJECT + "'),like(topic:subject,'positive*'))")
                ))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    sendMessagesOverRest(thingId, FEATURE_ID_1, SUBJECT, CONTENT_TYPE_IMAGE, 1);
                    sendMessagesOverRest(thingId, FEATURE_ID_1, "non-matching-subject", CONTENT_TYPE_IMAGE, 1);
                    sendMessagesOverRest(thingId, FEATURE_ID_2, "positive-vibes", CONTENT_TYPE_IMAGE, 1);
                    sendMessagesOverRest(thingId, FEATURE_ID_1, "positiveNess", CONTENT_TYPE_IMAGE, 1);
                    sendMessagesOverRest(thingId, FEATURE_ID_2, "what-is-this", CONTENT_TYPE_IMAGE, 1);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
        awaitLatchFalse(negativeLatch);
    }

    @Test
    public void testFilterMessagesSubscriptionUsingExtraFieldsAndRql()
            throws InterruptedException, TimeoutException, ExecutionException {
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch negativeLatch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttributes(Attributes.newBuilder()
                        .set("location", "basement")
                        .set("pure-awesomeness", true)
                        .build()
                )
                .setFeature(FEATURE_ID_1).build();

        registerForMessages(dittoClient.live(), "dummyKitchen", latch);
        registerForMessages(dittoClient.live(), "dummyBasement", negativeLatch);

        dittoClient.twin().create(thing)
                .thenCompose(thingAsPersisted -> dittoClient.live().startConsumption(
                        Options.Consumption.extraFields(JsonFieldSelector.newInstance("/attributes")),
                        Options.Consumption
                                .filter("and(like(topic:subject,'dummy*'),eq(attributes/location,'kitchen'))")
                ))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    // we expect that this message is NOT received, as the location is still "basement"
                    sendMessagesOverRest(thingId, FEATURE_ID_1, "dummyBasement", CONTENT_TYPE_IMAGE, 1);

                    // we need to sleep here as the above message consumption would do a roundtrip in order to retrieve
                    // the "extra" field "attributes" which the below statement could overtake and already set the
                    // location to "kitchen":
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        // ingore
                    }

                    dittoClient.twin().forId(thingId).putAttribute("location", "kitchen")
                            .toCompletableFuture()
                            .join();

                    // we expect that this message is received, as the location is now "kitchen"
                    sendMessagesOverRest(thingId, FEATURE_ID_1, "dummyKitchen", CONTENT_TYPE_IMAGE, 1);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
        awaitLatchFalse(negativeLatch);
    }

    private <U> Consumer<RepliableMessage<?, U>> getMessageConsumer(final CountDownLatch latch,
            final ThingId thingId,
            final String subject) {

        return rawMessage ->
        {
            LOGGER.debug("Message received for '{}': {}", subject, rawMessage);
            assertThat(rawMessage.getEntityId().toString()).isEqualTo(thingId.toString());
            assertThat(rawMessage.getFeatureId()).contains(FEATURE_ID_1);
            assertThat(rawMessage.getContentType().get()).startsWith(CONTENT_TYPE_IMAGE);
            latch.countDown();
        };
    }

    /*
     * Create a thing giving ditto-client and user1 full access and user3 READ+ADMINISTRATE access.
     */
    private Thing newThing(final ThingId thingId) {
        return ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setFeature(FEATURE_ID_1)
                .setFeature(FEATURE_ID_2)
                .build();
    }

}
