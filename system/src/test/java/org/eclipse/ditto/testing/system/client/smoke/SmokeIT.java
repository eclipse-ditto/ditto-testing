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
package org.eclipse.ditto.testing.system.client.smoke;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.testing.system.client.util.ThingFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(Acceptance.class)
public final class SmokeIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmokeIT.class);

    private DittoClient dittoClient;
    private DittoClient dittoClientSubscriber;

    @Before
    public void setUp() {
        TestingContext testingContextForDittoClientSubscriber =
                TestingContext.newInstance(serviceEnv.getDefaultTestingContext().getSolution(),
                        serviceEnv.getDefaultTestingContext().getOAuthClient());

        dittoClient = newDittoClient(serviceEnv.getDefaultTestingContext().getOAuthClient());
        dittoClientSubscriber = newDittoClient(testingContextForDittoClientSubscriber.getOAuthClient());
    }

    @After
    public void tearDown() {
        shutdownClient(dittoClient);
        shutdownClient(dittoClientSubscriber);
    }

    @Test
    public void testClient() throws ExecutionException, InterruptedException, TimeoutException {
        final CountDownLatch createLatch = new CountDownLatch(1);
        final CountDownLatch modifiedLatch = new CountDownLatch(1);
        final CountDownLatch deleteLatch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId),
                serviceEnv.getDefaultTestingContext().getOAuthClient(),
                serviceEnv.getTestingContext2().getOAuthClient());

        LOGGER.info("The THING to be inserted: {}", thing.toJsonString(JsonSchemaVersion.V_2));

        registerForThingChange(dittoClientSubscriber, thingId, createLatch, modifiedLatch, deleteLatch);

        dittoClientSubscriber.twin().startConsumption()
                .thenCompose(aVoid1 -> dittoClient.twin().create(thing, policy))
                .thenCompose(thingAsPersisted -> {
                    LOGGER.info("Created Thing: {}", thingAsPersisted);
                    return dittoClient.twin().forId(thingId).retrieve();
                })
                .thenCompose(thingAsPersisted -> {
                    assertThat(thingAsPersisted.getEntityId()).isEqualTo(thing.getEntityId());
                    return dittoClient.twin().forId(thingId).putAttribute("hello", "world");
                })
                .thenCompose(thingAsPersisted -> dittoClient.twin().forId(thingId).delete())
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                    dittoClient.twin().delete(thingId);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(createLatch);
        awaitLatchTrue(modifiedLatch);
        awaitLatchTrue(deleteLatch);
    }

    @Test
    public void testREST() throws Exception {
        final CountDownLatch createLatch = new CountDownLatch(1);
        final CountDownLatch modifiedLatch = new CountDownLatch(1);
        final CountDownLatch deleteLatch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setSubject(serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();

        LOGGER.info("The THING to be inserted: {}", thing.toJsonString(JsonSchemaVersion.V_2));

        registerForThingChange(dittoClientSubscriber, thingId, createLatch, modifiedLatch, deleteLatch);

        dittoClientSubscriber.twin().startConsumption()
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                    try {
                        putThingWithPolicy(2, thing, policy, JsonSchemaVersion.V_2)
                                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken())
                                .expectingHttpStatus(HttpStatus.CREATED)
                                .fire();
                        final Thing modified = thing.setAttribute("hello", "world");
                        putThing(2, modified, JsonSchemaVersion.V_2)
                                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                                .fire();
                        deleteThing(2, thingId)
                                .withJWT(serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken())
                                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                                .fire();
                    } catch (final Exception e) {
                        LOGGER.error("Error in Test", e);
                    }
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(createLatch);
        awaitLatchTrue(modifiedLatch);
        awaitLatchTrue(deleteLatch);
    }

    private void registerForThingChange(final DittoClient integrationClient, final ThingId thingId,
            final CountDownLatch createLatch, final CountDownLatch modifiedLatch, final CountDownLatch deleteLatch) {
        LOGGER.info("Registering for ThingChanges for thingId '{}'", thingId);
        integrationClient.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(), e ->
        {
            LOGGER.info("Received {} change event for {}", e.getAction(), e.getEntityId());
            switch (e.getAction()) {
                case CREATED:
                    createLatch.countDown();
                    break;
                case UPDATED:
                    modifiedLatch.countDown();
                    break;
                case DELETED:
                    deleteLatch.countDown();
                    break;
            }
        });
    }

}
