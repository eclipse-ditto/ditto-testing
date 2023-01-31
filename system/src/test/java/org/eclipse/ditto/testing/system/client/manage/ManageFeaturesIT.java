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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.changes.ChangeAction;
import org.eclipse.ditto.client.options.Option;
import org.eclipse.ditto.client.options.Options;
import org.eclipse.ditto.client.twin.Twin;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Features;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.response.Response;

/**
 * Test for features management.
 */
public class ManageFeaturesIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManageFeaturesIT.class);

    private static final String FEATURE_ID_1 = "feature_id_1";
    private static final String FEATURE_ID_EMPTY = "feature_id_empty";
    private static final Feature FEATURE_EMPTY =
            ThingsModelFactory.newFeatureBuilder().withId(FEATURE_ID_EMPTY).build();
    private static final String FEATURE_ID_EXISTING = "feature_id_existing";

    private static final JsonPointer PROPERTY_POINTER = JsonFactory.newPointer("pointer");
    private static final JsonValue PROPERTY_VALUE = JsonFactory.newValue("value");

    private static final JsonPointer PROPERTY_POINTER_NEW = JsonFactory.newPointer("pointer_new");
    private static final JsonValue PROPERTY_VALUE_NEW = JsonFactory.newValue("value_new");

    private static final Feature FEATURE_EXISTING = ThingsModelFactory.newFeatureBuilder()
            .properties(FeatureProperties.newBuilder()
                    .set(PROPERTY_POINTER, PROPERTY_VALUE)
                    .build())
            .withId(FEATURE_ID_EXISTING)
            .build();
    private static final Feature FEATURE_EXISTING_UPDATED = ThingsModelFactory.newFeatureBuilder()
            .properties(FeatureProperties.newBuilder()
                    .set(PROPERTY_POINTER_NEW, PROPERTY_VALUE_NEW)
                    .build())
            .withId(FEATURE_ID_EXISTING)
            .build();

    private static final Feature FEATURE_1 = ThingsModelFactory.newFeatureBuilder()
            .properties(FeatureProperties.newBuilder()
                    .set(PROPERTY_POINTER, PROPERTY_VALUE)
                    .build())
            .withId(FEATURE_ID_1)
            .build();
    private static final Feature FEATURE_1_UPDATED = ThingsModelFactory.newFeatureBuilder()
            .properties(FeatureProperties.newBuilder()
                    .set(PROPERTY_POINTER_NEW, PROPERTY_VALUE_NEW)
                    .build())
            .withId(FEATURE_ID_1)
            .build();
    private static final Feature FEATURE_1_MERGED = ThingsModelFactory.newFeatureBuilder()
            .properties(FeatureProperties.newBuilder()
                    .set(PROPERTY_POINTER, PROPERTY_VALUE)
                    .set(PROPERTY_POINTER_NEW, PROPERTY_VALUE_NEW)
                    .build())
            .withId(FEATURE_ID_1)
            .build();

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

    @Test
    public void testCreateFeatures() throws InterruptedException, TimeoutException, ExecutionException {

        final CountDownLatch latch = new CountDownLatch(1);
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        dittoClient.twin().create(randomThing(thingId), newPolicy(PolicyId.of(thingId), user, subscriber))
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForFeaturesChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.CREATED.equals(change.getAction())) {
                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> {
                    final Features features = ThingsModelFactory.newFeatures(FEATURE_1);

                    return dittoClient.twin().forId(thingId).setFeatures(features);
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).retrieve())
                .thenAccept(thing -> {
                    assertThingContainsFeature(thing, FEATURE_1);

                    dittoClient.twin().delete(thingId);
                })
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
    public void testDeleteFeatures() throws InterruptedException, TimeoutException, ExecutionException {

        final CountDownLatch latch = new CountDownLatch(1);
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        dittoClient.twin()
                .create(randomThingWithFeature(thingId), newPolicy(PolicyId.of(thingId), user, subscriber))
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForThingChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (change.isPartial()) {

                                    if (ChangeAction.DELETED.equals(change.getAction())) {
                                        latch.countDown();
                                    }
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).deleteFeatures())
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).retrieve())
                .thenCompose(thing -> {
                    assertThat(thing).isNotNull();
                    assertThat(thing.getFeatures()).isEmpty();

                    return dittoClient.twin().delete(thingId);
                })
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
    public void testCreateEmptyFeature() throws InterruptedException, TimeoutException, ExecutionException {

        final CountDownLatch latch = new CountDownLatch(1);
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        dittoClient.twin()
                .create(randomThingWithFeature(thingId), newPolicy(PolicyId.of(thingId), user, subscriber))
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForFeatureChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.CREATED.equals(change.getAction())
                                        && change.getFeature().getId().equals(FEATURE_ID_EMPTY)) {

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).putFeature(FEATURE_EMPTY))
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).retrieve())
                .thenAccept(thing -> assertThingContainsFeature(thing, FEATURE_EMPTY))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    dittoClient.twin().delete(thingId);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testCreateFeature() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        dittoClient.twin().create(randomThingWithFeature(thingId), newPolicy(PolicyId.of(thingId), user, subscriber))
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForFeatureChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.CREATED.equals(change.getAction())
                                        && change.getFeature().getId().equals(FEATURE_ID_1)) {

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).putFeature(FEATURE_1))
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).retrieve())
                .thenAccept(thing -> {
                    assertThingContainsFeature(thing, FEATURE_1);
                })
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    dittoClient.twin().delete(thingId);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testSetFeatureWithOptionExistsFalseFailsWhenItAlreadyExists() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final Twin twin = dittoClient.twin();
        final Option<Boolean> doesNotExist = Options.Modify.exists(false);
        twin.create(randomThingWithFeature(thingId), newPolicy(PolicyId.of(thingId), user, subscriber))
                .thenCompose(aVoid -> twin.forId(thingId).putFeature(FEATURE_EXISTING_UPDATED, doesNotExist))
                .handle((unused, throwable) -> {
                    if (null != throwable) {
                        LOGGER.info("Received expected Exception: '{}'", throwable.getMessage());
                        latch.countDown();
                    }
                    return null;
                })
                .thenCompose(aVoid -> twin.forId(thingId).retrieve())
                .thenAccept(thing -> assertThingContainsOnlyFeature(thing, FEATURE_EXISTING))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    twin.delete(thingId);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testSetFeatureWithOptionExistsTrueFailsWhenItDoesNotExist() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final Twin twin = dittoClient.twin();
        final Option<Boolean> exists = Options.Modify.exists(true);
        twin.create(randomThingWithFeature(thingId), newPolicy(PolicyId.of(thingId), user, subscriber))
                .thenCompose(aVoid -> twin.forId(thingId).putFeature(FEATURE_1, exists))
                .handle((unused, throwable) -> {
                    if (null != throwable) {
                        LOGGER.info("Received expected Exception: '{}'", throwable.getMessage());
                        latch.countDown();
                    }
                    return null;
                })
                .thenCompose(aVoid -> twin.forId(thingId).retrieve())
                .thenAccept(thing -> assertThingContainsOnlyFeature(thing, FEATURE_EXISTING))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    twin.delete(thingId);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testMergeFeatureWithOptionExistsFalseFailsWhenItAlreadyExists() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final Thing thingWithFeature = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setFeature(FEATURE_EXISTING)
                .build();

        final Twin twin = dittoClientV2.twin();
        final Option<Boolean> doesNotExist = Options.Modify.exists(false);
        twin.create(thingWithFeature)
                .thenCompose(aVoid -> twin.forId(thingId).mergeFeature(FEATURE_EXISTING_UPDATED, doesNotExist))
                .handle((unused, throwable) -> {
                    if (null != throwable) {
                        LOGGER.info("Received expected Exception: '{}'", throwable.getMessage());
                        latch.countDown();
                    }
                    return null;
                })
                .thenCompose(aVoid -> twin.forId(thingId).retrieve())
                .thenAccept(thing -> assertThingContainsOnlyFeature(thing, FEATURE_EXISTING))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    twin.delete(thingId);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testMergeFeatureWithOptionExistsTrueFailsWhenItDoesNotExist() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final Thing thingWithFeature = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setFeature(FEATURE_EXISTING)
                .build();

        final Twin twin = dittoClientV2.twin();
        final Option<Boolean> exists = Options.Modify.exists(true);
        twin.create(thingWithFeature)
                .thenCompose(aVoid -> twin.forId(thingId).mergeFeature(FEATURE_1, exists))
                .handle((unused, throwable) -> {
                    if (null != throwable) {
                        LOGGER.info("Received expected Exception: '{}'", throwable.getMessage());
                        latch.countDown();
                    }
                    return null;
                })
                .thenCompose(aVoid -> twin.forId(thingId).retrieve())
                .thenAccept(thing -> assertThingContainsOnlyFeature(thing, FEATURE_EXISTING))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    twin.delete(thingId);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testModifyFeature() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        dittoClient.twin().create(randomThingWithFeature(thingId), newPolicy(PolicyId.of(thingId), user, subscriber))
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForFeatureChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.UPDATED.equals(change.getAction())
                                        && change.getFeature().getId().equals(FEATURE_ID_1)) {

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> {
                    // create feature
                    return dittoClient.twin().forId(thingId).putFeature(FEATURE_1);
                })
                .thenCompose(aVoid -> {
                    // update feature
                    return dittoClient.twin().forId(thingId).putFeature(FEATURE_1_UPDATED);
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).retrieve())
                .thenAccept(thing -> assertThingContainsFeature(thing, FEATURE_1_UPDATED))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                    dittoClient.twin().delete(thingId);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testMergeFeature() throws Exception {

        final CountDownLatch latch = new CountDownLatch(2);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final Thing thingWithFeature = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setFeature(FEATURE_EXISTING)
                .build();

        final Policy policy = PoliciesModelFactory.newPolicy(TestConstants.Policy.DEFAULT_POLICY)
                .setSubjectFor("DEFAULT", subscriber.getSubject());

        dittoClientV2.twin().create(thingWithFeature, policy)
                .thenCompose(thing -> {
                    dittoClientSubscriberV2.twin().forId(thingId)
                            .registerForFeatureChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.MERGED.equals(change.getAction())
                                        && change.getFeature().getId().equals(FEATURE_ID_1)) {

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriberV2.twin().startConsumption();
                })
                .thenCompose(aVoid -> {
                    // create feature
                    return dittoClientV2.twin().forId(thingId).mergeFeature(FEATURE_1);
                })
                .thenCompose(aVoid -> {
                    // update feature
                    return dittoClientV2.twin().forId(thingId).mergeFeature(FEATURE_1_UPDATED);
                })
                .thenCompose(aVoid -> dittoClientV2.twin().forId(thingId).retrieve())
                .thenAccept(thing -> assertThingContainsFeature(thing, FEATURE_1_MERGED))
                .whenComplete((unused, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                    dittoClientV2.twin().delete(thingId);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testDeleteFeature() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        dittoClient.twin().create(randomThingWithFeature(thingId), newPolicy(PolicyId.of(thingId), user, subscriber))
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForFeatureChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.DELETED.equals(change.getAction())) {

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).deleteFeature(FEATURE_ID_EXISTING))
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).retrieve())
                .thenCompose(thing -> {
                    assertThat(thing).isNotNull();
                    assertThat(thing.getFeatures()).isPresent();
                    assertThat(thing.getFeatures().get().getFeature(FEATURE_ID_EXISTING)).isEmpty();

                    return dittoClient.twin().delete(thingId);
                })
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    private static String getFeatureFromRest(final ThingId thingId, final String featureId) {
        try {
            final boolean getFeature = featureId != null;

            final GetMatcher matcher;
            if (getFeature) {
                matcher = getFeature(2, thingId, featureId);
            } else {
                matcher = getFeatures(2, thingId);
            }
            final Response response = matcher.expectingHttpStatus(HttpStatus.OK).fire();
            return response.getBody().asString();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Thing randomThingWithFeature(final ThingId thingId) {
        return ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setFeature(FEATURE_EXISTING)
                .build();
    }

    private Thing randomThing(final ThingId thingId) {
        return ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .build();
    }

    private void assertThingContainsFeature(final Thing thing, final Feature expectedFeature) {
        assertThingContainsFeature(thing, expectedFeature, false);
    }

    private void assertThingContainsOnlyFeature(final Thing thing, final Feature expectedFeature) {
        assertThingContainsFeature(thing, expectedFeature, true);
    }

    private void assertThingContainsFeature(final Thing thing, final Feature expectedFeature,
            final boolean isOnlyFeature) {
        assertThat(thing).isNotNull();
        final ThingId theThingId = thing.getEntityId()
                .orElseThrow(() -> new AssertionError("Missing thingId!"));
        assertThat(thing.getFeatures()).isPresent();
        final String expectedFeatureId = expectedFeature.getId();
        assertThat(thing.getFeatures().get().getFeature(expectedFeatureId)).isPresent();
        if (isOnlyFeature) {
            assertThat(thing.getFeatures().get()).hasSize(1);
        }
        final Feature actualFeature = thing.getFeatures().get().getFeature(expectedFeatureId).get();
        assertThat(actualFeature).isEqualTo(expectedFeature);

        final String restFeatureStr = getFeatureFromRest(theThingId, expectedFeatureId);
        final Feature featureFromRest =
                ThingsModelFactory.newFeatureBuilder(restFeatureStr).useId(expectedFeatureId).build();
        assertThat(featureFromRest).isEqualTo(expectedFeature);
    }
}
