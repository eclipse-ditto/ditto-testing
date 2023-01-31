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

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.live.messages.MessageRegistration;
import org.eclipse.ditto.messages.model.MessageDirection;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.testing.system.client.util.ThingFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests deregistration of a message handler.
 */
public class HandlerDeregistrationIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(HandlerDeregistrationIT.class);

    private static final String DUMMY_SUBJECT = "dummySubject";
    private static final String CONTENT_TYPE_IMAGE = "application/raw+image";

    private DittoClient dittoClient;

    private ThingId thingId;

    private static void registerForMessages(final String registrationId, final MessageRegistration registration,
            final String subject, final Semaphore messageSemaphore) {
        registration.registerForMessage(registrationId, subject, ByteBuffer.class, message ->
        {
            LOGGER.info("Message received: {}", message);
            messageSemaphore.release();
        });
    }

    @Before
    public void setUp() throws Exception {
        LOGGER.info("-----> Running Test: {}", name.getMethodName());

        final AuthClient user1 = serviceEnv.getDefaultTestingContext().getOAuthClient();
        dittoClient = newDittoClient(user1);

        thingId = ThingId.of(idGenerator().withRandomName());

        final Thing thing = ThingFactory.newThing(thingId);
        dittoClient.twin().create(thing).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @After
    public void tearDown() {
        if (dittoClient != null) {
            try {
                dittoClient.twin().delete(thingId).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (final Exception e) {
                LOGGER.error("Error during Test", e);
            } finally {
                shutdownClient(dittoClient);
            }
        }
    }

    /**
     * Registers and deregisters a message handler and checks that message are no longer retrieved after deregistration.
     */
    @Test
    public void testDeregisterMessageHandler() throws Exception {
        final Semaphore sem = new Semaphore(0);
        final String registrationId = UUID.randomUUID().toString();

        registerForMessages(registrationId, dittoClient.live().forId(thingId), DUMMY_SUBJECT, sem);

        dittoClient.live().startConsumption()
                .whenComplete((aVoid, throwable) ->
                {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    final String payload =
                            new com.eclipsesource.json.JsonObject().add("createdAt", System.currentTimeMillis())
                                    .toString();

                    postMessage(2, thingId, MessageDirection.FROM, DUMMY_SUBJECT, CONTENT_TYPE_IMAGE, payload,
                            "0").fire();

                    try {
                        // make sure that registration was successful before testing deregistration
                        Assert.assertTrue(sem.tryAcquire(2, TimeUnit.SECONDS));

                        final boolean test = dittoClient.live().deregister(registrationId);
                        LOGGER.info("Deregistering registration '{}' was: {}", registrationId, test);

                        sem.drainPermits();
                        postMessage(2, thingId, MessageDirection.FROM, DUMMY_SUBJECT, CONTENT_TYPE_IMAGE,
                                payload, "0").fire();

                        // verify: handler must not have been called again
                        Assert.assertFalse(sem.tryAcquire(2, TimeUnit.SECONDS));
                    } catch (final InterruptedException e) {

                        LOGGER.error("Error in Test", e);
                    }
                })
                .toCompletableFuture()
                .get(LATCH_TIMEOUT, TimeUnit.SECONDS);
    }
}
