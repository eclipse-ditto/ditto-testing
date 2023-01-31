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

import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.changes.ChangeAction;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.json.JsonValueContainer;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.testing.system.client.util.ThingFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManageAttributesIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManageAttributesIT.class);

    private AuthClient user;
    private AuthClient subscriber;

    private DittoClient dittoClient;
    private DittoClient dittoClientV2;
    private DittoClient dittoClientSubscriber;
    private DittoClient dittoClientSubscriberV2;

    @Before
    public void setUp() {
        LOGGER.info("-----> Running Test: {}", name.getMethodName());

        user = serviceEnv.getDefaultTestingContext().getOAuthClient();
        subscriber = serviceEnv.getTestingContext2().getOAuthClient();

        dittoClient = newDittoClient(user);
        dittoClientV2 = newDittoClientV2(user);
        dittoClientSubscriber = newDittoClient(subscriber);
        dittoClientSubscriberV2 = newDittoClientV2(subscriber);
    }

    @After
    public void tearDown() {
        shutdownClient(dittoClient);
        shutdownClient(dittoClientV2);
        shutdownClient(dittoClientSubscriber);
        shutdownClient(dittoClientSubscriberV2);
    }

    @Test
    public void attributeActions() throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(3);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonPointer path = JsonFactory.newPointer("bla");
        final String addValue = "addValue";
        final String updateValue = "updateValue";
        final String mergeValue = "mergeValue";

        final Policy policy = PoliciesModelFactory.newPolicy(TestConstants.Policy.DEFAULT_POLICY)
                .setSubjectFor("DEFAULT", subscriber.getSubject());

        dittoClientV2.twin().create(thingId, policy).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        startConsumptionAndWait(dittoClientSubscriberV2.twin());
        dittoClientSubscriberV2.twin().forId(thingId)
                .registerForAttributesChanges(UUID.randomUUID().toString(), change -> {
                    LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                    if (changePathEndsWithPointer(change, path)) {
                        latch.countDown();
                    }
                });

        dittoClientV2.twin().forId(thingId).putAttribute(path, addValue) // CREATE
                .thenCompose(unused -> dittoClientV2.twin().forId(thingId).putAttribute(path, updateValue)) // UPDATE
                .thenCompose(unused -> dittoClientV2.twin().forId(thingId).mergeAttribute(path, mergeValue)) // MERGE
                .thenCompose(unused -> dittoClientV2.twin().forId(thingId).deleteAttribute(path)) // DELETE
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                }).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void changeAttributeString() throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonPointer path = JsonFactory.newPointer("attributePath");
        final String value = "attributeValue";

        final Thing thingToCreate = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        dittoClient.twin().create(thingToCreate, policy)
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForAttributesChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.CREATED.equals(change.getAction()) &&
                                        changePathEndsWithPointer(change, path)) {
                                    assertThat(change.getValue()).isPresent();
                                    assertThat(change.getValue().map(JsonValue::asString).get()).isEqualTo(value);

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).putAttribute(path, value))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                }).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void mergeAttributeString() throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonPointer path = JsonFactory.newPointer("attributePath");
        final String value = "attributeValue";

        final Policy policy = PoliciesModelFactory.newPolicy(TestConstants.Policy.DEFAULT_POLICY)
                .setSubjectFor("DEFAULT", subscriber.getSubject());

        dittoClientV2.twin().create(thingId, policy)
                .thenCompose(thing -> {
                    dittoClientSubscriberV2.twin().forId(thingId)
                            .registerForAttributesChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.MERGED.equals(change.getAction()) &&
                                        changePathEndsWithPointer(change, path)) {
                                    assertThat(change.getValue()).isPresent();
                                    assertThat(change.getValue().map(JsonValue::asString).get()).isEqualTo(value);

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriberV2.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClientV2.twin().forId(thingId).mergeAttribute(path, value))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                }).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void changeAttributeInt() throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonPointer path = JsonFactory.newPointer("intPath");
        final int value = 42;

        final Thing thingToCreate = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        dittoClient.twin().create(thingToCreate, policy)
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForAttributesChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.CREATED.equals(change.getAction()) &&
                                        changePathEndsWithPointer(change, path)) {
                                    assertThat(change.getValue()).isPresent();
                                    assertThat(change.getValue().map(JsonValue::asInt).get()).isEqualTo(value);

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).putAttribute(path, value))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                }).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void changeAttributeDouble() throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonPointer path = JsonFactory.newPointer("doublePath");
        final double value = 2d;

        final Thing thingToCreate = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        dittoClient.twin().create(thingToCreate, policy)
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForAttributesChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.CREATED.equals(change.getAction()) &&
                                        changePathEndsWithPointer(change, path)) {
                                    assertThat(change.getValue()).isPresent();
                                    assertThat(change.getValue().map(JsonValue::asDouble).get()).isEqualTo(value);

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).putAttribute(path, value))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                }).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void changeAttributeBoolean() throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonPointer path = JsonFactory.newPointer("booleanPath");
        final boolean value = true;

        final Thing thingToCreate = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        dittoClient.twin().create(thingToCreate, policy)
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForAttributesChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.CREATED.equals(change.getAction()) &&
                                        changePathEndsWithPointer(change, path)) {
                                    assertThat(change.getValue()).isPresent();
                                    assertThat(change.getValue().map(JsonValue::asBoolean).get()).isEqualTo(value);

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).putAttribute(path, value))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                }).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void changeAttributeLong() throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonPointer path = JsonFactory.newPointer("longPath");
        final long value = 2L;

        final Thing thingToCreate = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        dittoClient.twin().create(thingToCreate, policy)
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForAttributesChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.CREATED.equals(change.getAction()) &&
                                        changePathEndsWithPointer(change, path)) {
                                    assertThat(change.getValue()).isPresent();
                                    assertThat(change.getValue().map(JsonValue::asLong).get()).isEqualTo(value);

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).putAttribute(path, value))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                }).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void changeAttributeNullValue() throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonPointer path = JsonFactory.newPointer("nullValuePath");
        final JsonValue value = JsonFactory.nullLiteral();

        final Thing thingToCreate = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        dittoClient.twin().create(thingToCreate, policy)
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForAttributesChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.info("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.CREATED.equals(change.getAction()) &&
                                        changePathEndsWithPointer(change, path)) {
                                    assertThat(change.getValue()).isPresent();
                                    assertThat(change.getValue().get()).isEqualTo(value);

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).putAttribute(path, value))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                }).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void changeAttributeNonNullValue() throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonPointer path = JsonFactory.newPointer("nonNullPath");
        final JsonValue value = JsonFactory.newValue(2.2);

        final Thing thingToCreate = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        dittoClient.twin().create(thingToCreate, policy)
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForAttributesChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.CREATED.equals(change.getAction()) &&
                                        changePathEndsWithPointer(change, path)) {
                                    assertThat(change.getValue()).isPresent();
                                    assertThat(change.getValue().get()).isEqualTo(value);

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).putAttribute(path, value))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                }).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void changeAttributesNullObject() throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonObject value = JsonFactory.nullObject();

        final Thing thingToCreate = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        dittoClient.twin().create(thingToCreate, policy)
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForAttributesChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.UPDATED.equals(change.getAction())) {
                                    assertThat(change.getValue()).contains(ThingsModelFactory.nullAttributes());
                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).setAttributes(value))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                }).toCompletableFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void mergeAttributeNullValue() throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(2);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonPointer path = JsonFactory.newPointer("nullValuePath");
        final JsonValue value = JsonFactory.newValue(42);
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(path, value)
                .build();

        final Policy policy = PoliciesModelFactory.newPolicy(TestConstants.Policy.DEFAULT_POLICY)
                .setSubjectFor("DEFAULT", subscriber.getSubject());


        final JsonValue nullLiteral = JsonFactory.nullLiteral();

        dittoClientV2.twin().create(thing, policy)
                .thenCompose(created -> {
                    dittoClientSubscriberV2.twin().forId(thingId)
                            .registerForAttributesChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.MERGED.equals(change.getAction()) &&
                                        changePathEndsWithPointer(change, path)) {
                                    assertThat(change.getValue()).isPresent();
                                    assertThat(change.getValue().get()).isEqualTo(nullLiteral);

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriberV2.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClientV2.twin().forId(thingId).mergeAttribute(path, nullLiteral))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                })
                .thenCompose(aVoid -> dittoClientV2.twin().forId(thingId).retrieve())
                .thenAccept(recieved -> recieved.getAttributes()
                        .filter(JsonValueContainer::isEmpty)
                        .ifPresentOrElse(aVoid -> latch.countDown(), () -> LOGGER.info("Attribute is not deleted"))
                )
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void mergeAttributesNullObject() throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(2);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonPointer path = JsonFactory.newPointer("nullValuePath");
        final JsonValue value = JsonFactory.newValue(42);
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(path, value)
                .build();

        final Policy policy = PoliciesModelFactory.newPolicy(TestConstants.Policy.DEFAULT_POLICY)
                .setSubjectFor("DEFAULT", subscriber.getSubject());

        final JsonObject nullLiteral = JsonFactory.nullObject();

        dittoClientV2.twin().create(thing, policy)
                .thenCompose(created -> {
                    dittoClientSubscriberV2.twin().forId(thingId)
                            .registerForAttributesChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.MERGED.equals(change.getAction()) &&
                                        changePathEndsWithPointer(change, path)) {
                                    assertThat(change.getValue()).isPresent();
                                    assertThat(change.getValue().get()).isEqualTo(nullLiteral);

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriberV2.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClientV2.twin().forId(thingId).mergeAttribute(path, nullLiteral))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                })
                .thenCompose(aVoid -> dittoClientV2.twin().forId(thingId).retrieve())
                .thenAccept(received -> received.getAttributes()
                        .filter(JsonValueContainer::isEmpty)
                        .ifPresentOrElse(aVoid -> latch.countDown(), () -> LOGGER.info("Attribute is not deleted"))
                )
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void changeAttributesNonNullObject() throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonObject value = createAttributesJson();

        final Thing thingToCreate = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        dittoClient.twin().create(thingToCreate, policy)
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForAttributesChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.UPDATED.equals(change.getAction())) {
                                    assertThat(change.getValue().map(JsonValue::asObject)).contains(value);

                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).setAttributes(value))
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
    public void deleteAttributes() throws InterruptedException, ExecutionException, TimeoutException {
        final CountDownLatch latch = new CountDownLatch(2);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final JsonObject value = createAttributesJson();

        final Thing thingToCreate = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        dittoClient.twin().create(thingToCreate, policy)
                .thenCompose(thing -> {
                    dittoClientSubscriber.twin().forId(thingId)
                            .registerForAttributesChanges(UUID.randomUUID().toString(), change -> {
                                LOGGER.debug("[{}] Received event: {}", name.getMethodName(), change);

                                if (ChangeAction.DELETED.equals(change.getAction())) {
                                    latch.countDown();
                                }
                            });

                    return dittoClientSubscriber.twin().startConsumption();
                })
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).setAttributes(value))
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).deleteAttributes())
                .thenCompose(aVoid -> dittoClient.twin().forId(thingId).retrieve())
                .whenComplete((thingAsPersisted, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    assertThat(thingAsPersisted.getAttributes()).isEmpty();
                    latch.countDown();
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void modifyAttributesFailsIfThingGetsTooLarge() {

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final int overhead = 150;

        final JsonObject largeAttributes = JsonObject.newBuilder()
                .set("a", "a".repeat(DEFAULT_MAX_THING_SIZE - overhead))
                .build();

        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId)
                .setAttributes(largeAttributes)
                .build();

        final JsonObject largeAttributesB = JsonObject.newBuilder()
                .set("a", "a".repeat(overhead))
                .build();

        expectThingTooLargeExceptionIfThingGetsTooLarge(dittoClient, thing,
                handle -> handle.putAttribute("b", largeAttributesB));
    }

    @Test
    public void mergeAttributesFailsIfThingGetsTooLarge() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final int overhead = 150;

        final JsonObject largeAttributes = JsonObject.newBuilder()
                .set("a", "a".repeat(DEFAULT_MAX_THING_SIZE - overhead))
                .build();

        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId)
                .setAttributes(largeAttributes)
                .build();

        final JsonObject largeAttributesB = JsonObject.newBuilder()
                .set("a", "a".repeat(overhead))
                .build();

        expectThingTooLargeExceptionIfThingGetsTooLarge(dittoClientV2, thing,
                handle -> handle.mergeAttribute("b", largeAttributesB));
    }

    private JsonObject createAttributesJson() {
        return JsonFactory.newObjectBuilder()
                .set(JsonFactory.newPointer("intPath"), TIMEOUT_SECONDS)
                .set(JsonFactory.newPointer("stringPath"), "stringValue")
                .set(JsonFactory.newPointer("doublePath"), 2.2)
                .set(JsonFactory.newPointer("longPath"), Long.valueOf(TIMEOUT_SECONDS))
                .set(JsonFactory.newPointer("booleanPath"), true)
                .set(JsonFactory.newPointer("nullValuePath"), JsonFactory.nullLiteral())
                .set(JsonFactory.newPointer("nonNullValuePath"), JsonFactory.newValue(2.2))
                .set(JsonFactory.newPointer("nullObjectPath"), JsonFactory.nullObject())
                .set(JsonFactory.newPointer("arrayPath"),
                        JsonFactory.newArrayBuilder().add(2.2, 3.3).add("hi").build())
                .build();
    }

}
