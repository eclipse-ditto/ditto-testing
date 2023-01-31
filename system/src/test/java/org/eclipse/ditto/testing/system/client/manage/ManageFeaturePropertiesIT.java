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
package org.eclipse.ditto.testing.system.client.manage;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.eclipse.ditto.base.model.exceptions.DittoRuntimeException;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.changes.ChangeAction;
import org.eclipse.ditto.client.twin.TwinThingHandle;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.testing.system.client.util.ThingFactory;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.exceptions.FeatureNotAccessibleException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for managing {@link org.eclipse.ditto.things.model.FeatureProperties} with the {@link org.eclipse.ditto.client.DittoClient}.
 */
public class ManageFeaturePropertiesIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManageFeaturePropertiesIT.class);

    private static final String FEATURE_ID = "smokeDetector";
    private static final JsonPointer PROPERTY_JSON_POINTER = JsonFactory.newPointer("density");
    private static final JsonValue PROPERTY_JSON_VALUE = JsonFactory.newValue(42);
    private static final JsonValue PROPERTY_JSON_VALUE_2 = JsonFactory.newValue(41);
    private static final FeatureProperties FEATURE_PROPERTIES =
            ThingsModelFactory.newFeaturePropertiesBuilder().set(PROPERTY_JSON_POINTER, PROPERTY_JSON_VALUE).build();
    private static final FeatureProperties FEATURE_PROPERTIES_2 =
            ThingsModelFactory.newFeaturePropertiesBuilder().set(PROPERTY_JSON_POINTER, PROPERTY_JSON_VALUE_2).build();
    private static final Feature FEATURE = ThingsModelFactory.newFeature(FEATURE_ID);
    private static final Feature FEATURE_WITH_PROPERTIES = FEATURE.setProperties(FEATURE_PROPERTIES);

    private AuthClient user;
    private AuthClient subscriber;
    private DittoClient dittoClient;
    private DittoClient dittoClientSubscriber;
    private DittoClient dittoClientV2;
    private DittoClient dittoClientSubscriberV2;

    @Before
    public void setUp() {
        LOGGER.info("-----> Running Test: {}", name.getMethodName());

        user = serviceEnv.getDefaultTestingContext().getOAuthClient();
        subscriber = serviceEnv.getTestingContext2().getOAuthClient();

        dittoClient = newDittoClient(user);
        dittoClientSubscriber = newDittoClient(subscriber);
        dittoClientV2 = newDittoClientV2(user);
        dittoClientSubscriberV2 = newDittoClientV2(subscriber);
    }

    @After
    public void tearDown() {
        shutdownClient(dittoClient);
        shutdownClient(dittoClientSubscriber);
    }

    @Test
    public void modifyFeatureProperty() throws InterruptedException, ExecutionException, TimeoutException {

        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        dittoClient.twin()
                .create(thingId)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        final TwinThingHandle thingHandle = dittoClient.twin().forId(thingId);

        thingHandle.putFeature(FEATURE)
                .thenCompose(
                        aVoid -> thingHandle.forFeature(FEATURE_ID)
                                .putProperty(PROPERTY_JSON_POINTER, PROPERTY_JSON_VALUE))
                .thenCompose(aVoid -> thingHandle.forFeature(FEATURE_ID).retrieve())
                .thenAccept(feature -> {
                    assertThat(feature.getProperty(PROPERTY_JSON_POINTER)).isPresent();
                    assertThat(feature.getProperty(PROPERTY_JSON_POINTER).get()).isEqualTo(PROPERTY_JSON_VALUE);

                    latch.countDown();
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        if (!latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS)) {
            fail("Expected a Feature but received none.");
        }
    }

    @Test
    public void mergeFeatureProperty() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final TwinThingHandle thingHandle = dittoClientV2.twin().forId(thingId);

        dittoClientV2.twin()
                .create(thingId)
                .thenCompose(created -> thingHandle.putFeature(FEATURE_WITH_PROPERTIES))
                .thenCompose(
                        aVoid -> thingHandle.forFeature(FEATURE_ID)
                                .mergeProperty(PROPERTY_JSON_POINTER, PROPERTY_JSON_VALUE_2))
                .thenCompose(aVoid -> thingHandle.forFeature(FEATURE_ID).retrieve())
                .thenAccept(feature -> feature.getProperty(PROPERTY_JSON_POINTER).ifPresentOrElse(property -> {
                    assertThat(property).isEqualTo(PROPERTY_JSON_VALUE_2);
                    latch.countDown();
                }, () -> {
                    throw new AssertionError("Property pointer is missing");
                }));

        if (!latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS)) {
            fail("Expected a Feature but received none.");
        }
    }

    @Test
    public void modifyFeaturePropertyOnNonExistingFeature()
            throws InterruptedException, ExecutionException, TimeoutException {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        dittoClient.twin()
                .create(thingId)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        final TwinThingHandle thingHandle = dittoClient.twin().forId(thingId);

        try {

            thingHandle.forFeature(FEATURE_ID)
                    .putProperty(PROPERTY_JSON_POINTER, PROPERTY_JSON_VALUE)
                    .toCompletableFuture()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            fail("Expected CrRuntimeException");
        } catch (final ExecutionException e) {

            if (e.getCause() instanceof final DittoRuntimeException cre) {

                assertThat(cre.getErrorCode()).isEqualTo(FeatureNotAccessibleException.ERROR_CODE);
            } else {
                throw e;
            }
        }
    }

    @Test
    public void deleteFeatureProperty() throws InterruptedException, ExecutionException, TimeoutException {

        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        dittoClient.twin().create(thingId)
                .thenCompose(thing -> dittoClient.twin().forId(thingId).putFeature(FEATURE_WITH_PROPERTIES))
                .thenCompose(
                        aVoid -> dittoClient.twin()
                                .forId(thingId)
                                .forFeature(FEATURE_ID)
                                .deleteProperty(PROPERTY_JSON_POINTER))
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).forFeature(FEATURE_ID).retrieve())
                .thenCompose(feature -> {
                    assertThat(feature.getProperty(PROPERTY_JSON_POINTER)).isEmpty();

                    latch.countDown();

                    return dittoClient.twin().delete(thingId);
                })
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(latch.await(LATCH_TIMEOUT, SECONDS))
                .withFailMessage("Did not receive expected events within timeout.")
                .isTrue();
    }

    @Test
    public void modifyFeatureProperties() throws InterruptedException, TimeoutException, ExecutionException {

        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        dittoClient.twin()
                .create(thingId)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        final TwinThingHandle thingHandle = dittoClient.twin().forId(thingId);

        thingHandle.putFeature(FEATURE)
                .thenCompose(aVoid -> thingHandle.forFeature(FEATURE_ID).setProperties(FEATURE_PROPERTIES))
                .thenCompose(aVoid -> thingHandle.forFeature(FEATURE_ID).retrieve())
                .thenAccept(feature -> {
                    assertThat(feature.getProperties()).isPresent();
                    assertThat(feature.getProperties().get()).isEqualTo(FEATURE_PROPERTIES);

                    latch.countDown();
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        if (!latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS)) {

            fail("Expected a Feature but received none.");
        }
    }

    @Test
    public void mergeFeatureProperties() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final TwinThingHandle thingHandle = dittoClientV2.twin().forId(thingId);

        dittoClientV2.twin()
                .create(thingId)
                .thenCompose(created -> thingHandle.putFeature(FEATURE_WITH_PROPERTIES))
                .thenCompose(aVoid -> thingHandle.forFeature(FEATURE_ID).mergeProperties(FEATURE_PROPERTIES_2))
                .thenCompose(aVoid -> thingHandle.forFeature(FEATURE_ID).retrieve())
                .thenAccept(feature -> feature.getProperties().ifPresentOrElse(properties -> {
                    assertThat(properties).isEqualTo(FEATURE_PROPERTIES_2);
                    latch.countDown();
                }, () -> {
                    throw new AssertionError("Property pointer is missing");
                }));

        if (!latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS)) {
            fail("Expected a Feature but received none.");
        }
    }

    @Test
    public void deleteFeatureProperties() throws InterruptedException, TimeoutException, ExecutionException {

        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        dittoClient.twin()
                .create(thingId)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        final TwinThingHandle thingHandle = dittoClient.twin().forId(thingId);

        thingHandle.putFeature(FEATURE_WITH_PROPERTIES)
                .thenCompose(aVoid -> thingHandle.forFeature(FEATURE_ID).deleteProperties())
                .thenCompose(aVoid -> thingHandle.forFeature(FEATURE_ID).retrieve())
                .thenAccept(feature -> {
                    assertThat(feature.getProperties()).isEmpty();

                    latch.countDown();
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        if (!latch.await(LATCH_TIMEOUT, TimeUnit.SECONDS)) {

            fail("Expected a Feature but received none.");
        }
    }

    @Test
    public void registerForFeaturePropertyChangeOnAllProperties()
            throws InterruptedException, TimeoutException, ExecutionException {

        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId).setFeature(FEATURE);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        dittoClient.twin().create(thing, policy)
                .thenCompose(thingAsPersisted -> {
                    dittoClientSubscriber.twin().forFeature(thingId, FEATURE_ID)
                            .registerForPropertyChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.CREATED.equals(change.getAction())
                                        && changePathEndsWithPointer(change, PROPERTY_JSON_POINTER)) {

                                    latch.countDown();
                                }
                            });
                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).forFeature(FEATURE_ID)
                        .putProperty(PROPERTY_JSON_POINTER, PROPERTY_JSON_VALUE))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void registerForFeaturePropertyChangeOnSpecificProperty()
            throws InterruptedException, TimeoutException, ExecutionException {

        final CountDownLatch latch = new CountDownLatch(2);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final Thing thingWithFeature = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setFeature(FEATURE)
                .build();

        final Policy policy = PoliciesModelFactory.newPolicy(TestConstants.Policy.DEFAULT_POLICY)
                .setSubjectFor("DEFAULT", subscriber.getSubject());

        dittoClientV2.twin().create(thingWithFeature, policy)
                .thenCompose(thingAsPersisted -> {
                    dittoClientSubscriberV2.twin().forFeature(thingId, FEATURE_ID)
                            .registerForPropertyChanges(UUID.randomUUID().toString(), PROPERTY_JSON_POINTER, change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if ((ChangeAction.CREATED.equals(change.getAction()) ||
                                        ChangeAction.MERGED.equals(change.getAction()))
                                        && changePathEndsWithPointer(change, PROPERTY_JSON_POINTER)) {

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriberV2.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClientV2.twin().forId(thingId).forFeature(FEATURE_ID)
                        .putProperty(PROPERTY_JSON_POINTER, PROPERTY_JSON_VALUE))
                .thenCompose(aVoid -> dittoClientV2.twin().forId(thingId).forFeature(FEATURE_ID)
                        .mergeProperty(PROPERTY_JSON_POINTER, PROPERTY_JSON_VALUE_2))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void modifyFeaturePropertiesFailsIfThingGetsTooLarge() {

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final int overhead = 150;

        final JsonObject largeFeatureProperty = JsonObject.newBuilder()
                .set("a", "a".repeat(DEFAULT_MAX_THING_SIZE - overhead))
                .build();

        final Feature featureWithLargePropertyA = ThingsModelFactory
                .newFeature(FEATURE_ID)
                .setProperty("a", largeFeatureProperty);

        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId)
                .setFeature(featureWithLargePropertyA)
                .build();

        final Feature featureWithTooLargeProperties = featureWithLargePropertyA.setProperty("b", largeFeatureProperty);

        expectThingTooLargeExceptionIfThingGetsTooLarge(dittoClientV2, thing,
                twinThingHandle -> twinThingHandle.putFeature(featureWithTooLargeProperties));
    }

    @Test
    public void mergeFeaturePropertiesFailsIfThingGetsTooLarge() {

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final int overhead = 150;

        final JsonObject largeFeatureProperty = JsonObject.newBuilder()
                .set("a", "a".repeat(DEFAULT_MAX_THING_SIZE - overhead))
                .build();

        final Feature featureWithLargePropertyA = ThingsModelFactory
                .newFeature(FEATURE_ID)
                .setProperty("a", largeFeatureProperty);

        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId)
                .setFeature(featureWithLargePropertyA)
                .build();

        final Feature featureWithLargePropertyB = ThingsModelFactory
                .newFeature(FEATURE_ID)
                .setProperty("b", largeFeatureProperty);

        expectThingTooLargeExceptionIfThingGetsTooLarge(dittoClientV2, thing,
                twinThingHandle -> twinThingHandle.mergeFeature(featureWithLargePropertyB));

    }

}
