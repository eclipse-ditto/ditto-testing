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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;

import java.text.MessageFormat;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.live.messages.RepliableMessage;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.testing.system.client.util.ThingFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.response.Response;

/**
 * Integration test for receiving claim messages with the {@link org.eclipse.ditto.client.DittoClient}.
 */
public class HandleClaimMessagesIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(HandleClaimMessagesIT.class);

    private static final int MESSAGE_COUNT = 1;

    private static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";

    private static final String SUCCESS_MESSAGE = "success";
    private static final String ERROR_MESSAGE = "claiming failed";
    private static final String EXPECTED_PAYLOAD = "expectedPayload";
    private static final String TIMEOUT = "30";

    private DittoClient dittoClient;
    private ThingId thingId;

    @Before
    public void setUp() throws Exception {
        LOGGER.info("-----> Running Test: {}", name.getMethodName());

        final AuthClient user1 = serviceEnv.getDefaultTestingContext().getOAuthClient();

        dittoClient = newDittoClient(user1);

        thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);
        dittoClient.twin().create(thing)
                .whenComplete((thingAsPersisted, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);
    }

    @After
    public void tearDown() {
        if (dittoClient != null) {
            try {
                dittoClient.twin().delete(thingId)
                        .toCompletableFuture()
                        .get(TIMEOUT_SECONDS, SECONDS);
            } catch (final Exception e) {
                LOGGER.error("Error during Test", e);
            } finally {
                shutdownClient(dittoClient);
            }
        }
    }

    /**
     * Create one Thing and register for claim messages on it.
     */
    @Test
    public void receiveClaimMessageForThing() throws Exception {
        final CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);

        dittoClient.live().forId(thingId).registerForClaimMessage(UUID.randomUUID().toString(), message -> {
            LOGGER.info("receiveClaimMessageForThing(): Received claim message: {}", message);
            handleClaimMessage(message);
            latch.countDown();
        });

        dittoClient.live().startConsumption()
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        sendClaimMessagesOverRest(thingId, MESSAGE_COUNT);

        awaitLatchTrue(latch);
    }

    /**
     * Create one Thing and register for claim messages on all things.
     */
    @Test
    public void receiveClaimMessagesForAllThings() throws Exception {
        final CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);

        dittoClient.live().registerForClaimMessage(UUID.randomUUID().toString(), message -> {
            LOGGER.info("receiveClaimMessagesForAllThings(): Received claim message: {}", message);
            handleClaimMessage(message);
            latch.countDown();
        });

        dittoClient.live().startConsumption()
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        sendClaimMessagesOverRest(thingId, MESSAGE_COUNT);

        awaitLatchTrue(latch);
    }

    @Test
    public void receiveClaimMessagesWithoutPayload() throws Exception {
        final CountDownLatch latch = new CountDownLatch(MESSAGE_COUNT);

        dittoClient.live().forId(thingId).registerForClaimMessage(UUID.randomUUID().toString(), message -> {
            LOGGER.info("receiveClaimMessageWithoutPayload(): Received claim message: {}", message);
            assertThat(message.getPayload()).isEmpty();
            message.reply()
                    .httpStatus(HttpStatus.OK)
                    .payload(SUCCESS_MESSAGE)
                    .contentType(CONTENT_TYPE_TEXT_PLAIN)
                    .send();
            latch.countDown();
        });

        dittoClient.live().startConsumption()
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        sendClaimMessagesWithoutPayload(thingId, MESSAGE_COUNT);

        awaitLatchTrue(latch);
    }

    @Test
    public void claimThingReturnsAfterSpecifiedTimeout() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        dittoClient.live().forId(thingId).registerForClaimMessage(UUID.randomUUID().toString(), message -> {
            LOGGER.info("claimThingReturnsAfterSpecifiedTimeout(): Received claim message: {}", message);
            latch.countDown();
        });

        dittoClient.live().startConsumption()
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        sendClaimMessages(thingId, 1, "", "1", HttpStatus.REQUEST_TIMEOUT, any(String.class));

        awaitLatchTrue(latch);
    }

    @Test
    public void sendClaimMessageWithZeroTimeoutReturnsAccepted() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        dittoClient.live().forId(thingId).registerForClaimMessage(UUID.randomUUID().toString(), message -> {
            LOGGER.info("sendClaimMessageWithZeroTimeoutReturnsAccepted(): Received claim message: {}", message);
            latch.countDown();
        });

        dittoClient.live().startConsumption()
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        sendClaimMessages(thingId, 1, "", "0", HttpStatus.ACCEPTED, any(String.class));

        awaitLatchTrue(latch);
    }

    @Test
    public void claimThingWithPayloadAndCustomStatusCode() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final String imATeapot = "I'm a little teapot short and stout";

        dittoClient.live().forId(thingId).registerForClaimMessage(UUID.randomUUID().toString(), message -> {
            LOGGER.info("claimThingWithPayloadAndCustomStatusCode(): Received claim message: {}", message);
            message.reply()
                    .httpStatus(HttpStatus.IM_A_TEAPOT)
                    .payload(imATeapot)
                    .contentType(CONTENT_TYPE_TEXT_PLAIN)
                    .send();
            latch.countDown();
        });

        dittoClient.live().startConsumption()
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        sendClaimMessages(thingId, MESSAGE_COUNT, "", TIMEOUT, HttpStatus.IM_A_TEAPOT, equalTo(imATeapot));

        awaitLatchTrue(latch);
    }

    private static void handleClaimMessage(final RepliableMessage<?, Object> message) {
        final String messagePayload = message.getPayload()
                .filter(p -> p instanceof String)
                .map(p -> (String) p)
                .orElseThrow(() -> new IllegalArgumentException("Payload does not have the expected format (String)."));
        if (EXPECTED_PAYLOAD.equals(messagePayload)) {
            LOGGER.info("Actual payload '{}' matches expected payload.", messagePayload);
            message.reply()
                    .httpStatus(HttpStatus.OK)
                    .payload(SUCCESS_MESSAGE)
                    .contentType(CONTENT_TYPE_TEXT_PLAIN)
                    .send();
        } else {
            LOGGER.warn("Actual payload '{}' does not match expected payload '{}', sending failure response.",
                    messagePayload, EXPECTED_PAYLOAD);
            message.reply()
                    .httpStatus(HttpStatus.BAD_REQUEST)
                    .payload(ERROR_MESSAGE)
                    .send();
        }
    }

    private static void sendClaimMessagesWithoutPayload(final ThingId thingId, final int count) {
        sendClaimMessages(thingId, count, "", TIMEOUT, HttpStatus.OK, equalTo(SUCCESS_MESSAGE));
    }

    private static void sendClaimMessagesOverRest(final ThingId thingId, final int count) {
        sendClaimMessages(thingId, count, EXPECTED_PAYLOAD, TIMEOUT, HttpStatus.OK, equalTo(SUCCESS_MESSAGE));
    }

    private static void sendClaimMessages(final ThingId thingId, final int count, final String payload,
            final String timeout, final HttpStatus expectedStatusCode, final Matcher<String> expectedBody) {
        final String url = claimUrl(thingId);
        LOGGER.info("Start sending '{}' claim messages to '{}'", count, thingId);
        for (int i = 0; i < count; i++) {
            try {
                final Response response = post(url, CONTENT_TYPE_TEXT_PLAIN, payload.getBytes())
                        .withParam("timeout", timeout)
                        .expectingHttpStatus(expectedStatusCode)
                        .expectingBody(expectedBody)
                        .fire();

                LOGGER.info("Response: {}", response.getStatusLine());
                if (LOGGER.isDebugEnabled()) {
                    response.prettyPrint();
                }
            } catch (final Exception e) {
                LOGGER.error("Exception occurred when sending message", e);
            }
        }
    }

    private static String claimUrl(final ThingId thingId) {
        return MessageFormat.format("{0}/things/{1}/inbox/claim", thingsServiceUrl(2), thingId);
    }
}
