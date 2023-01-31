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
import static org.hamcrest.Matchers.instanceOf;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.signals.commands.exceptions.CommandTimeoutException;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.live.messages.RepliableMessage;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.messages.model.Message;
import org.eclipse.ditto.messages.model.MessageDirection;
import org.eclipse.ditto.messages.model.MessagePayloadSizeTooLargeException;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.testing.system.client.util.ClientFactory;
import org.eclipse.ditto.testing.system.client.util.ThingFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for sending messages.
 */
public final class ClientSendMessageIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientSendMessageIT.class);

    private static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";
    private static final String CONTENT_TYPE_APPLICATION_JSON = "application/json";
    private static final String CONTENT_TYPE_APPLICATION_OCTET_STREAM = "application/octet-stream";

    private static final String FEATURE_ID = "fluxCapacitor";
    private static final String SUBJECT = "subject";
    private static final String PAYLOAD = "payload";
    private static final JsonValue JSON_PAYLOAD = JsonFactory.readFrom("\"" + PAYLOAD + "\"");

    private DittoClient dittoClientOwner;
    private DittoClient dittoClientWriter;
    private DittoClient dittoClientReader1;
    private DittoClient dittoClientReader2;

    private ThingId thingId;

    @Before
    public void setUp() throws Exception {
        final AuthClient owner = serviceEnv.getDefaultTestingContext().getOAuthClient();
        final AuthClient writer = serviceEnv.getTestingContext2().getOAuthClient();
        final AuthClient reader1 = serviceEnv.getTestingContext3().getOAuthClient();
        final AuthClient reader2 = serviceEnv.getTestingContext4().getOAuthClient();

        dittoClientOwner = newDittoClient(owner);
        dittoClientWriter = newDittoClient(writer);
        dittoClientReader1 = newDittoClient(reader1);
        dittoClientReader2 = newDittoClient(reader2);

        thingId = ThingId.of(idGenerator().withRandomName());

        final Policy policy =
                newPolicy(PolicyId.of(thingId), Arrays.asList(reader1, reader2, owner), Arrays.asList(writer, owner));

        final Thing thing = ThingFactory.newThing(thingId)
                .setFeatures(ThingFactory.newFeatures(FEATURE_ID));

        dittoClientOwner.twin()
                .create(thing, policy)
                .whenComplete((thingAsPersisted, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @After
    public void tearDown() {
        if (dittoClientOwner != null) {
            try {
                dittoClientOwner.twin().delete(thingId)
                        .toCompletableFuture()
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (final Exception e) {
                LOGGER.error("Error during Test", e);
            } finally {
                shutdownClient(dittoClientOwner);
                shutdownClient(dittoClientWriter);
                shutdownClient(dittoClientReader1);
                shutdownClient(dittoClientReader2);
            }
        }
    }

    @Test
    public void testMessageToThing() {
        final CompletableFuture<?> future = registerForRawMessages(MessageDirection.TO);

        consumeMessages().whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            LOGGER.info("Sending live message to thing <{}> ...", thingId);
            dittoClientWriter.live()
                    .message()
                    .to(thingId)
                    .subject(SUBJECT)
                    .payload(ByteBuffer.wrap(PAYLOAD.getBytes()))
                    .send();
        });

        future.join();
    }

    @Test
    public void testMessageToThingAndExpectTimeout() {
        LOGGER.info("Sending live message to thing <{}> with timeout and expect error response ...", thingId);
        final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
        dittoClientWriter.live()
                .message()
                .to(thingId)
                .subject(SUBJECT)
                .timeout(Duration.ofSeconds(2))
                .payload(ByteBuffer.wrap(PAYLOAD.getBytes()))
                .send((response, t) -> throwableRef.set(t));
        Awaitility.await("timeout exception")
                .untilAtomic(throwableRef, instanceOf(CommandTimeoutException.class));
    }

    @Test
    public void testMessageToThing_thingHandle() {
        final CompletableFuture<?> future = registerForRawMessages(MessageDirection.TO);

        consumeMessages().whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            dittoClientWriter.live()
                    .forId(thingId)
                    .message()
                    .to()
                    .subject(SUBJECT)
                    .payload(ByteBuffer.wrap(PAYLOAD.getBytes()))
                    .send();
        });

        future.join();
    }

    @Test
    public void testMessageToThingWithoutFeatureId() {
        final CompletableFuture<?> future = registerForJsonMessages(MessageDirection.TO);

        consumeMessages().whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            dittoClientWriter.live()
                    .message()
                    .to(thingId)
                    .subject(SUBJECT)
                    .payload(JSON_PAYLOAD)
                    .send();
        });

        future.join();
    }

    @Test
    public void testMessageToThingWithoutFeatureId_thingHandle() {
        final CompletableFuture<?> future = registerForJsonMessages(MessageDirection.TO);

        consumeMessages().whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            dittoClientWriter.live()
                    .forId(thingId)
                    .message()
                    .to()
                    .subject(SUBJECT)
                    .payload(JSON_PAYLOAD)
                    .send();
        });

        future.join();
    }

    @Test
    public void testRawMessageFromThing() {
        final CompletableFuture<?> future = registerForRawMessages(MessageDirection.FROM);

        consumeMessages().whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            dittoClientWriter.live()
                    .message()
                    .from(thingId)
                    .subject(SUBJECT)
                    .payload(ByteBuffer.wrap(PAYLOAD.getBytes()))
                    .send();
        });

        future.join();
    }

    @Test
    public void testRawMessageFromThing_thingHandle() {
        final CompletableFuture<?> future = registerForRawMessages(MessageDirection.FROM);

        consumeMessages().whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            dittoClientWriter.live()
                    .forId(thingId)
                    .message()
                    .from()
                    .subject(SUBJECT)
                    .payload(ByteBuffer.wrap(PAYLOAD.getBytes()))
                    .send();
        });

        future.join();
    }

    @Test
    public void testStringMessageFromThing() throws InterruptedException {
        final CountDownLatch latch = registerForStringMessages(MessageDirection.FROM);

        consumeMessages().whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            dittoClientWriter.live()
                    .message()
                    .from(thingId)
                    .subject(SUBJECT)
                    .payload(PAYLOAD)
                    .send();
        });

        awaitLatchTrue(latch);
    }

    @Test
    public void testStringMessageFromThing_thingHandle() throws InterruptedException {
        final CountDownLatch latch = registerForStringMessages(MessageDirection.FROM);

        consumeMessages().whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            dittoClientWriter.live()
                    .forId(thingId)
                    .message()
                    .from()
                    .subject(SUBJECT)
                    .payload(PAYLOAD)
                    .send();
        });

        awaitLatchTrue(latch);
    }

    @Test
    public void testJsonMessageFromThing() {
        final CompletableFuture<?> future = registerForJsonMessages(MessageDirection.FROM);

        consumeMessages().whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            dittoClientWriter.live()
                    .message()
                    .from(thingId)
                    .subject(SUBJECT)
                    .payload(JSON_PAYLOAD)
                    .send();
        });

        future.join();
    }

    @Test
    public void testJsonMessageFromThing_thingHandle() {
        final CompletableFuture<?> future = registerForJsonMessages(MessageDirection.FROM);

        consumeMessages().whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            dittoClientWriter.live()
                    .forId(thingId)
                    .message()
                    .from()
                    .subject(SUBJECT)
                    .payload(JSON_PAYLOAD)
                    .send();
        });

        future.join();
    }

    @Test
    public void testCustomTypeMessageFromThing() {
        final Duration payload = Duration.ofSeconds(1337);
        final CompletableFuture<?> future = registerForCustomTypeMessages(MessageDirection.FROM, payload);

        consumeMessages().whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            dittoClientWriter.live()
                    .message()
                    .from(thingId)
                    .subject(SUBJECT)
                    .payload(payload)
                    .send();
        });

        future.join();
    }

    @Test
    public void testCustomTypeMessageFromThing_thingHandle() {
        final Duration payload = Duration.ofDays(42);
        final CompletableFuture<?> future = registerForCustomTypeMessages(MessageDirection.FROM, payload);

        consumeMessages().whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            dittoClientWriter.live()
                    .forId(thingId)
                    .message()
                    .from()
                    .subject(SUBJECT)
                    .payload(payload)
                    .send();
        });

        future.join();
    }

    @Test
    public void testMessageToFeature() {
        final CompletableFuture<?> future = registerForFeatureMessages(MessageDirection.TO);

        consumeMessages().whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            dittoClientWriter.live()
                    .forFeature(thingId, FEATURE_ID)
                    .message()
                    .to()
                    .subject(SUBJECT)
                    .payload(JSON_PAYLOAD)
                    .send();
        });

        future.join();
    }

    @Test
    public void testMessageFromFeature() {
        final CompletableFuture<?> future = registerForFeatureMessages(MessageDirection.FROM);

        consumeMessages().whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            dittoClientWriter.live()
                    .forFeature(thingId, FEATURE_ID)
                    .message()
                    .from()
                    .subject(SUBJECT)
                    .payload(JSON_PAYLOAD)
                    .send();
        });

        future.join();
    }

    @Test
    public void testSendMessageFailsIfMessagePayloadTooLarge() {

        final CompletableFuture<?> senderFuture = new CompletableFuture<>();

        // this will never complete
        registerForJsonMessages(MessageDirection.TO);

        final JsonObject largePayload = JsonObject.newBuilder()
                .set("a", "a".repeat(DEFAULT_MAX_MESSAGE_SIZE))
                .build();

        consumeMessages().whenComplete((aVoid, throwable) -> dittoClientWriter.live()
                .forId(thingId)
                .message()
                .to()
                .subject("tooLarge4U")
                .payload(largePayload)
                .send((response, throwable2) -> {
                    try {
                        if (null != throwable2) {
                            LOGGER.info("Received expected Exception: '{}'", throwable2.getMessage());
                            assertThat(throwable2).isInstanceOf(MessagePayloadSizeTooLargeException.class);
                            senderFuture.complete(null);
                        } else {
                            throw new NullPointerException("Did not get the expected exception. Got: " + response);
                        }
                    } catch (final Throwable e) {
                        senderFuture.completeExceptionally(e);
                    }
                }));

        // receiverLatch should NOT reach zero:
        senderFuture.join();
    }

    private CompletionStage<Void> consumeMessages() {
        final CompletableFuture<Void> client1Consuming =
                dittoClientReader1.live().startConsumption().toCompletableFuture();
        final CompletableFuture<Void> client2Consuming =
                dittoClientReader2.live().startConsumption().toCompletableFuture();

        return CompletableFuture.allOf(client1Consuming, client2Consuming);
    }

    private CompletableFuture<Void> registerForRawMessages(final MessageDirection direction) {
        final CompletableFuture<Void> future1 = new CompletableFuture<>();
        final CompletableFuture<Void> future2 = new CompletableFuture<>();
        dittoClientReader1.live().forId(thingId).registerForMessage(UUID.randomUUID().toString(), SUBJECT,
                ByteBuffer.class, rawMessageValidator(future1, direction));
        dittoClientReader2.live().forId(thingId).registerForMessage(UUID.randomUUID().toString(), SUBJECT,
                ByteBuffer.class, rawMessageValidator(future2, direction));
        return CompletableFuture.allOf(future1, future2);
    }

    private CountDownLatch registerForStringMessages(final MessageDirection direction) {
        final CountDownLatch readersReceivedMessage = new CountDownLatch(2);
        final Consumer<RepliableMessage<String, String>> validator =
                stringMessageValidator(readersReceivedMessage, direction);
        dittoClientReader1.live().forId(thingId).registerForMessage(UUID.randomUUID().toString(), SUBJECT,
                String.class, validator);
        dittoClientReader2.live().forId(thingId).registerForMessage(UUID.randomUUID().toString(), SUBJECT,
                String.class, validator);
        return readersReceivedMessage;
    }

    private CompletableFuture<Void> registerForJsonMessages(final MessageDirection direction) {
        final CompletableFuture<Void> future1 = new CompletableFuture<>();
        final CompletableFuture<Void> future2 = new CompletableFuture<>();
        dittoClientReader1.live().forId(thingId).registerForMessage(UUID.randomUUID().toString(), SUBJECT,
                JsonValue.class, jsonMessageValidator(future1, direction, null));
        dittoClientReader2.live().forId(thingId).registerForMessage(UUID.randomUUID().toString(), SUBJECT,
                JsonValue.class, jsonMessageValidator(future2, direction, null));
        return CompletableFuture.allOf(future1, future2);
    }

    private CompletableFuture<Void> registerForCustomTypeMessages(final MessageDirection direction,
            final Duration expectedPayload) {

        final CompletableFuture<Void> future1 = new CompletableFuture<>();
        final CompletableFuture<Void> future2 = new CompletableFuture<>();
        dittoClientReader1.live().forId(thingId).registerForMessage(UUID.randomUUID().toString(), SUBJECT,
                Duration.class, customPayloadMessageValidator(future1, direction, expectedPayload));
        dittoClientReader2.live().forId(thingId).registerForMessage(UUID.randomUUID().toString(), SUBJECT,
                Duration.class, customPayloadMessageValidator(future2, direction, expectedPayload));
        return CompletableFuture.allOf(future1, future2);
    }

    private CompletableFuture<Void> registerForFeatureMessages(final MessageDirection direction) {
        final CompletableFuture<Void> future1 = new CompletableFuture<>();
        final CompletableFuture<Void> future2 = new CompletableFuture<>();
        dittoClientReader1.live().forFeature(thingId, FEATURE_ID).registerForMessage(UUID.randomUUID().toString(),
                SUBJECT, JsonValue.class, jsonMessageValidator(future1, direction, FEATURE_ID));
        dittoClientReader2.live().forFeature(thingId, FEATURE_ID).registerForMessage(UUID.randomUUID().toString(),
                SUBJECT, JsonValue.class, jsonMessageValidator(future2, direction, FEATURE_ID));
        return CompletableFuture.allOf(future1, future2);
    }

    private <U> Consumer<RepliableMessage<ByteBuffer, U>> rawMessageValidator(
            final CompletableFuture<?> assertionFuture,
            final MessageDirection direction) {

        return message -> {
            try {
                LOGGER.debug("Got raw-message in direction '{}': {}", direction, message);
                validateCommonProperties(message, direction, CONTENT_TYPE_APPLICATION_OCTET_STREAM);

                assertThat(message.getPayload()).contains(ByteBuffer.wrap(PAYLOAD.getBytes()));

                assertionFuture.complete(null);
            } catch (final Throwable e) {
                assertionFuture.completeExceptionally(e);
            }
        };
    }

    private <U> Consumer<RepliableMessage<String, U>> stringMessageValidator(final CountDownLatch successLatch,
            final MessageDirection direction) {

        return message -> {
            LOGGER.debug("Got String-message in direction '{}': {}", direction, message);
            validateCommonProperties(message, direction, CONTENT_TYPE_TEXT_PLAIN);

            assertThat(message.getPayload()).contains(PAYLOAD);

            successLatch.countDown();
        };
    }

    private <U> Consumer<RepliableMessage<JsonValue, U>> jsonMessageValidator(final CompletableFuture<?> future,
            final MessageDirection direction, final String featureId) {

        return message -> {
            try {
                LOGGER.debug("Got Json-message in direction '{}': {}", direction, message);
                validateCommonProperties(message, direction, CONTENT_TYPE_APPLICATION_JSON);

                if (featureId != null) {
                    assertThat(message.getFeatureId()).contains(featureId);
                }

                assertThat(message.getPayload().toString()).contains("\"" + PAYLOAD + "\"");
                future.complete(null);
            } catch (final Throwable e) {
                future.completeExceptionally(e);
            }
        };
    }

    private <U> Consumer<RepliableMessage<Duration, U>> customPayloadMessageValidator(
            final CompletableFuture<?> assertionFuture,
            final MessageDirection direction,
            final Duration expectedPayload) {

        return message -> {
            try {
                LOGGER.debug("Got custom PAYLOAD (Duration) message in direction '{}': {}", direction, message);
                validateCommonProperties(message, direction, ClientFactory.DURATION_CONTENT_TYPE);

                assertThat(message.getPayload()).contains(expectedPayload);
                assertionFuture.complete(null);
            } catch (final Throwable e) {
                assertionFuture.completeExceptionally(e);
            }
        };
    }

    private <T> void validateCommonProperties(final Message<T> message, final MessageDirection direction,
            final String contentType) {

        assertThat(message.getEntityId().toString()).isEqualTo(thingId.toString());
        assertThat(message.getSubject()).isEqualTo(SUBJECT);
        assertThat(message.getDirection()).isEqualTo(direction);

        final Optional<String> contentTypeOptional = message.getContentType();
        if (contentTypeOptional.isPresent()) {
            final String actualContentType = contentTypeOptional.get();
            LOGGER.debug("validateCommonProperties: expected content-type: {} - actual: {}", contentType,
                    actualContentType);
            assertThat(actualContentType).isEqualTo(contentType).as("Content-Type is the expected one");
        }
    }

}
