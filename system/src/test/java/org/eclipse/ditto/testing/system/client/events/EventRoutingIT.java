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
package org.eclipse.ditto.testing.system.client.events;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.json.JsonFactory.newObjectBuilder;
import static org.eclipse.ditto.json.JsonFactory.newPointer;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.changes.ChangeAction;
import org.eclipse.ditto.client.changes.FeaturesChange;
import org.eclipse.ditto.client.changes.ThingChange;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.testing.system.client.util.ThingFactory;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.Features;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.exceptions.ThingNotAccessibleException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tests that change events are propagates upwards and downwards correctly. E.g.:
 * <ul>
 * <li>The user adds a registration for {@link org.eclipse.ditto.client.changes.ThingChange}s -> he gets notified when:
 * <ul>
 * <li>The complete Thing changes (CREATE, UPDATE, DELETE)</li>
 * <li>All attributes of the Thing changes at once</li>
 * <li>Single attributes change</li>
 * <li>All features of the Thing changes at once</li>
 * <li>A single feature changes</li>
 * <li>Single feature properties change</li>
 * </ul>
 * </li>
 * <li>The user adds a registration for {@link org.eclipse.ditto.client.changes.Change}s on a specific attribute ->
 * he gets notified when:
 * <ul>
 * <li>This specific attributes changes (CREATE, UPDATE, DELETE)</li>
 * <li>The attribute changes implicitly because a parent JsonObject attribute is changed completely</li>
 * <li>The attribute changes implicitly because all attributes are changed</li>
 * <li>The attribute changes implicitly because the Thing changes</li>
 * </ul>
 * </li>
 * </ul>
 * Test Runtrip: Local run against cloud foundry dev with parameter <b>-Dtest.environment=cloud-dev</b>
 * <p>
 * send modify/delete feature(s) --> retrieve thing, verify feature(s) correctly modified/deleted --> Rest API
 * of Thing service, check the values of feature(s)
 * <p>
 * listen to feature(s) changes --> verify get the event feature(s) modified/deleted
 */
public class EventRoutingIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventRoutingIT.class);

    private static final String ATTRIBUTE_ABC = "abc";
    private static final String ATTRIBUTE_ABC_VALUE = "def";
    private static final String ATTRIBUTE_FOO = "foo";
    private static final boolean ATTRIBUTE_FOO_VALUE = false;

    private static final String FEATURE_ID_1 = "feature_id_1";
    private static final Feature FEATURE_1 = ThingsModelFactory.newFeature(FEATURE_ID_1)
            .setProperties(ThingsModelFactory.newFeaturePropertiesBuilder()
                    .set("one", 1)
                    .set("two", 2)
                    .build());
    private static final String FEATURE_ID_2 = "feature_id_2";
    private static final Feature FEATURE_2 = ThingsModelFactory.newFeature(FEATURE_ID_2)
            .setProperties(ThingsModelFactory.newFeaturePropertiesBuilder()
                    .set("complex",
                            newObjectBuilder()
                                    .set("bum", "lux")
                                    .build())
                    .build());

    private AuthClient user;
    private AuthClient userSubscriber;

    private DittoClient client;
    private DittoClient clientSubscriber;

    @Rule
    public Timeout timeout = new Timeout(1, TimeUnit.MINUTES);

    @Before
    public void setUp() {
        LOGGER.info("-----> Running Test: {}", name.getMethodName());

        user = serviceEnv.getDefaultTestingContext().getOAuthClient();
        userSubscriber = serviceEnv.getTestingContext2().getOAuthClient();

        client = newDittoClient(user);
        LOGGER.info("Created dittoClient");
        clientSubscriber = newDittoClient(userSubscriber);
        LOGGER.info("Created dittoClientSubscriber");
    }

    @After
    public void tearDown() {
        shutdownClient(client);
        shutdownClient(clientSubscriber);
    }

    @Test
    public void testUpwardsRegisterForThingChangeWhenThingIsCreated() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final Thing thing = ThingFactory.newThing(thingId).setPolicyId(policyId);
        final Policy policy = newPolicy(policyId, user, userSubscriber);

        clientSubscriber.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(),
                thingChange -> {
                    LOGGER.debug("received ThingChange {}", thingChange);

                    if (ChangeAction.CREATED.equals(thingChange.getAction())) {
                        assertThat(thingChange.getThing().flatMap(Thing::getEntityId)).hasValue(thingId);
                        assertThat(thingChange.isPartial()).isFalse();
                        assertThat((Object) thingChange.getPath()).isEqualTo(newPointer(""));
                        assertThat(thingChange.getThing()).hasValue(thing);
                        assertThat(thingChange.getValue()).hasValue(thing.toJson(JsonSchemaVersion.V_2));

                        latch.countDown();
                    }
                });

        startConsumingChanges();

        client.twin().create(thing, policy)
                .thenCompose(thingAsPersisted -> client.twin().forId(thingId).retrieve())
                .whenComplete((thingAsPersisted, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    // Client verify
                    assertThat(thingAsPersisted).isEqualTo(thing);

                    // REST API verify
                    final Thing thingFromRest = ThingsModelFactory.newThingBuilder(getThingFromRest(thingId)).build();
                    assertThat(thingFromRest).isEqualTo(thing);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testMultipleChangeHandlersAreInvokedOnSingleChange() throws Exception {
        final int amountOfSubscriptions = 7;
        final CountDownLatch latch = new CountDownLatch(amountOfSubscriptions);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final Thing thing = ThingFactory.newThing(thingId).setPolicyId(policyId);
        final Policy policy = newPolicy(policyId, user, userSubscriber);

        final Consumer<ThingChange> consumer = thingChange -> {
            LOGGER.debug("received ThingChange {}", thingChange);

            if (ChangeAction.CREATED.equals(thingChange.getAction())) {
                assertThat(thingChange.getThing().flatMap(Thing::getEntityId)).hasValue(thingId);
                assertThat(thingChange.isPartial()).isFalse();
                assertThat((Object) thingChange.getPath()).isEqualTo(newPointer(""));
                assertThat(thingChange.getThing()).hasValue(thing);
                assertThat(thingChange.getValue()).hasValue(thing.toJson(JsonSchemaVersion.V_2));

                latch.countDown();
            }
        };

        for (int i = 0; i < amountOfSubscriptions; i++) {
            if (i % 2 == 0) {
                clientSubscriber.twin().registerForThingChanges(UUID.randomUUID().toString(), consumer);
            } else {
                clientSubscriber.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(),
                        consumer);
            }
        }

        startConsumingChanges();

        // only create the Thing once:
        client.twin().create(thing, policy).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);

        awaitLatchTrue(latch);
    }


    @Test
    public void testUpwardsRegisterForThingChangeWhenThingIsDeleted() throws Exception {
        final CountDownLatch latch = new CountDownLatch(2);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, userSubscriber);

        // prepare: create the thing
        client.twin().create(thing, policy).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);

        clientSubscriber.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(),
                thingChange -> {
                    LOGGER.info("testUpwardsRegisterForThingChangeWhenThingIsDeleted: received ThingChange {}",
                            thingChange);

                    if (ChangeAction.DELETED.equals(thingChange.getAction())) {
                        assertThat(thingChange.getEntityId().toString()).isEqualTo(thingId.toString());
                        assertThat(thingChange.isPartial()).isFalse();
                        assertThat((Object) thingChange.getPath()).isEqualTo(
                                newPointer("")); // empty path on ThingChange
                        assertThat(thingChange.getThing()).isEmpty();
                        assertThat(thingChange.getValue()).isEmpty();

                        latch.countDown();
                    } else {
                        LOGGER.warn("testUpwardsRegisterForThingChangeWhenThingIsDeleted: action is NOT 'DELETED': {}",
                                thingChange.getAction());
                    }
                });

        startConsumingChanges();

        client.twin().delete(thingId).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);

        // Client verify
        client.twin().forId(thingId).retrieve().whenComplete((thingAsPersisted, ex) -> {
            LOGGER.info(
                    "testUpwardsRegisterForThingChangeWhenThingIsDeleted: retrieve deleted Thing returned Thing: {}",
                    thingAsPersisted);
            assertThat(thingAsPersisted).isNull();

            assertThat(ex).isNotNull();
            LOGGER.info(
                    "testUpwardsRegisterForThingChangeWhenThingIsDeleted: retrieve deleted Thing returned Exception: {}",
                    ex.getMessage(), ex);
            assertThat(ex.getCause()).isInstanceOf(ThingNotAccessibleException.class);

            LOGGER.info("testUpwardsRegisterForThingChangeWhenThingIsDeleted: Count down latch");
            latch.countDown();
        });

        awaitLatchTrue(latch);
    }

    @Test
    public void testUpwardsRegisterForThingChangeWhenAttributesAreModified() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, userSubscriber);

        // prepare: create the thing
        client.twin().create(thing, policy).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);
        final JsonObject attributesToSet =
                ThingsModelFactory.newAttributesBuilder().set("foo", "bar").set("misc", 1).build();

        clientSubscriber.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(),
                thingChange -> {
                    LOGGER.debug("received ThingChange {}", thingChange);

                    if (ChangeAction.UPDATED.equals(thingChange.getAction())) {
                        assertThat(thingChange.getEntityId().toString()).isEqualTo(thingId.toString());
                        assertThat(thingChange.isPartial()).isTrue();
                        assertThat((Object) thingChange.getPath()).isEqualTo(
                                newPointer("attributes")); // attributes were changed
                        assertThat(thingChange.getValue().get().toString())
                                .isEqualTo(newObjectBuilder().set("attributes", attributesToSet).build().toString());

                        latch.countDown();
                    }
                });

        startConsumingChanges();

        // set the attributes
        client.twin().forId(thingId).setAttributes(attributesToSet).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testUpwardsRegisterForThingChangeWhenSingleAttributeIsModified() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, userSubscriber);

        // prepare: create the thing
        client.twin().create(thing, policy).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);

        final JsonPointer newAttribute = newPointer("newAttribute");
        final int simpleValue = 42;
        final JsonObject simple = newObjectBuilder().set("simple", simpleValue).build();
        final JsonObject complex = newObjectBuilder().set("complex", simple).build();
        final JsonObject buildObject = newObjectBuilder().set(newAttribute, complex).build();

        clientSubscriber.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(),
                thingChange -> {
                    LOGGER.debug("received ThingChange {}", thingChange);

                    if (ChangeAction.CREATED.equals(thingChange.getAction())) {
                        assertThat(thingChange.getEntityId().toString()).isEqualTo(thingId.toString());
                        assertThat(thingChange.isPartial()).isTrue();
                        assertThat((Object) thingChange.getPath()).isEqualTo(
                                newPointer("attributes").append(newAttribute));
                        assertThat(thingChange.getValue().get().toString())
                                .isEqualTo(newObjectBuilder().set("attributes", buildObject).build().toString());

                        latch.countDown();
                    }
                });

        startConsumingChanges();

        // set the attributes
        client.twin().forId(thingId).putAttribute(newAttribute, complex)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testUpwardsRegisterForThingChangeWhenSingleFeatureIsDeleted() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, userSubscriber);

        // prepare: create the thing
        client.twin().create(thing, policy).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);
        // add a Feature
        client.twin().forId(thingId)
                .putFeature(ThingsModelFactory.newFeature(FEATURE_ID_1))
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        clientSubscriber.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(),
                thingChange -> {
                    LOGGER.debug("received ThingChange {}", thingChange);

                    if (ChangeAction.DELETED.equals(thingChange.getAction())) {
                        assertThat(thingChange.getEntityId().toString()).isEqualTo(thingId.toString());
                        assertThat(thingChange.isPartial()).isTrue();
                        assertThat((Object) thingChange.getPath()).isEqualTo(newPointer("features/" + FEATURE_ID_1));
                        assertThat(thingChange.getValue()).isEmpty();

                        latch.countDown();
                    }
                });

        startConsumingChanges();

        // delete the Feature
        client.twin().forId(thingId).deleteFeature(FEATURE_ID_1).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testUpwardsRegisterForFeaturesChangesWhenSingleFeaturePropertyIsCreated() throws Exception {
        final CountDownLatch latch = new CountDownLatch(2);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, userSubscriber);

        // prepare: create the thing
        client.twin().create(thing, policy).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);
        // add a Feature
        client.twin().forId(thingId) //
                .putFeature(ThingsModelFactory.newFeature(FEATURE_ID_1))
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        final JsonPointer fooPointer = newPointer("foo");
        final String fooValue = "bar";
        final JsonObject expectedChangedObject = newObjectBuilder().set(FEATURE_ID_1,
                newObjectBuilder().set("properties",
                        newObjectBuilder().set(fooPointer, fooValue).build()).build())
                .build();

        final Consumer<FeaturesChange> featuresChangeConsumer = featuresChange -> {
            LOGGER.debug("received ThingChange {}", featuresChange);

            if (ChangeAction.CREATED.equals(featuresChange.getAction())
                    && changePathEndsWithPointer(featuresChange, fooPointer)) {
                assertThat((CharSequence) featuresChange.getEntityId()).isEqualTo(thingId);
                assertThat(featuresChange.getValue().get().toString()).isEqualTo(expectedChangedObject.toString());

                latch.countDown();
            }
        };

        startConsumingChanges();

        // register both the "global" consumer and the one for a specific thingId:
        clientSubscriber.twin()
                .registerForFeaturesChanges(UUID.randomUUID().toString(), featuresChangeConsumer);
        clientSubscriber.twin().forId(thingId).registerForFeaturesChanges(UUID.randomUUID().toString(),
                featuresChangeConsumer);

        // create a Feature property
        client.twin().forId(thingId).forFeature(FEATURE_ID_1)
                .putProperty(fooPointer, fooValue)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testUpwardsRegisterForFeatureChangesWhenSingleFeaturePropertyIsUpdated() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Policy policy = newPolicy(PolicyId.of(thingId), user, userSubscriber);
        final Features features = Features.newBuilder()
                .set(FEATURE_1)
                .set(FEATURE_2)
                .build();
        final Attributes attributes = ThingsModelFactory.newAttributesBuilder()
                .set(ATTRIBUTE_ABC, ATTRIBUTE_ABC_VALUE)
                .set(ATTRIBUTE_FOO, ATTRIBUTE_FOO_VALUE)
                .build();
        final Thing thing = ThingFactory.newThing(thingId)
                .setAttributes(attributes)
                .setFeatures(features);

        // prepare: create the thing
        client.twin().create(thing, policy).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);

        final JsonPointer fooPointer = newPointer("complex/bum");
        final String fooValue = "bar";
        final JsonObject expectedChangedObject = newObjectBuilder().set("properties",
                newObjectBuilder().set(fooPointer, fooValue).build()).build();

        clientSubscriber.twin().registerForFeatureChanges(UUID.randomUUID().toString(), FEATURE_ID_2,
                featureChange -> {
                    LOGGER.debug("received Change {}", featureChange);

                    if (ChangeAction.UPDATED.equals(featureChange.getAction())
                            && changePathEndsWithPointer(featureChange, fooPointer)) {
                        assertThat((CharSequence) featureChange.getEntityId()).isEqualTo(thingId);
                        assertThat(featureChange.isPartial()).isTrue();
                        assertThat(featureChange.getValue().get().toString()).isEqualTo(
                                expectedChangedObject.toString());

                        latch.countDown();
                    }
                });

        startConsumingChanges();

        // create a Feature property
        client.twin().forId(thingId).forFeature(FEATURE_ID_2)
                .putProperty(fooPointer, fooValue)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testDownwardsRegisterForSingleAttributeChangeWhenThingIsCreated() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Policy policy = newPolicy(PolicyId.of(thingId), user, userSubscriber);
        final Attributes attributes = ThingsModelFactory.newAttributesBuilder()
                .set(ATTRIBUTE_ABC, ATTRIBUTE_ABC_VALUE)
                .set(ATTRIBUTE_FOO, ATTRIBUTE_FOO_VALUE)
                .build();
        final Thing thing = ThingFactory.newThing(thingId).setAttributes(attributes);

        clientSubscriber.twin().forId(thingId).registerForAttributesChanges(UUID.randomUUID().toString(),
                attrChange -> {
                    LOGGER.debug("received Change {}", attrChange);

                    if (ChangeAction.CREATED.equals(attrChange.getAction())) {
                        assertThat((CharSequence) attrChange.getEntityId()).isEqualTo(thingId);
                        assertThat(attrChange.isFull()).isTrue();
                        assertThat((Object) attrChange.getPath()).isEqualTo(newPointer(""));
                        assertThat(attrChange.getValue().get().toString()).isEqualTo(
                                thing.getAttributes().get().toJsonString());

                        latch.countDown();
                    }
                });

        startConsumingChanges();

        // create the thing with attributes
        client.twin().create(thing, policy).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testDownwardsRegisterForSingleAttributeChangeWhenAttributesAreModified() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Policy policy = newPolicy(PolicyId.of(thingId), user, userSubscriber);
        final Attributes attributes = ThingsModelFactory.newAttributesBuilder()
                .set(ATTRIBUTE_ABC, ATTRIBUTE_ABC_VALUE)
                .set(ATTRIBUTE_FOO, ATTRIBUTE_FOO_VALUE)
                .build();
        final Thing thing = ThingFactory.newThing(thingId).setAttributes(attributes);

        // prepare: create the thing with attributes
        client.twin().create(thing, policy).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);
        final String newAbcValue = "bumlux";
        final Attributes newAttributes =
                ThingsModelFactory.newAttributesBuilder().set(ATTRIBUTE_ABC, newAbcValue).build();

        final JsonPointer abcPointer = newPointer(ATTRIBUTE_ABC);

        clientSubscriber.twin().forId(thingId).registerForAttributeChanges(UUID.randomUUID().toString(),
                abcPointer, attrChange -> {
                    LOGGER.debug("received Change {}", attrChange);

                    if (ChangeAction.UPDATED.equals(attrChange.getAction())) {
                        assertThat((CharSequence) attrChange.getEntityId()).isEqualTo(thingId);
                        assertThat(attrChange.isFull()).isTrue();
                        assertThat((Object) attrChange.getPath()).isEqualTo(newPointer(""));
                        assertThat(attrChange.getValue().get().asString()).isEqualTo(newAbcValue);

                        latch.countDown();
                    }
                });

        startConsumingChanges();

        // create the thing with attributes
        client.twin().forId(thingId).setAttributes(newAttributes).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void testDownwardsRegisterForNestedFeaturePropertyChangeWhenFeatureIsModified() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Policy policy = newPolicy(PolicyId.of(thingId), user, userSubscriber);
        final Features features = Features.newBuilder()
                .set(FEATURE_1)
                .set(FEATURE_2)
                .build();
        final Attributes attributes = ThingsModelFactory.newAttributesBuilder()
                .set(ATTRIBUTE_ABC, ATTRIBUTE_ABC_VALUE)
                .set(ATTRIBUTE_FOO, ATTRIBUTE_FOO_VALUE)
                .build();
        final Thing thing = ThingFactory.newThing(thingId)
                .setAttributes(attributes)
                .setFeatures(features);

        // prepare: create the thing with attributes
        client.twin().create(thing, policy).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);

        final JsonPointer complexBumPointer = newPointer("complex/bum");

        clientSubscriber.twin().forFeature(thingId, FEATURE_ID_2)
                .registerForPropertyChanges(UUID.randomUUID().toString(), complexBumPointer, propChange -> {
                    LOGGER.debug("received Change {}", propChange);

                    if (ChangeAction.UPDATED.equals(propChange.getAction())) {
                        assertThat((CharSequence) propChange.getEntityId()).isEqualTo(thingId);
                        assertThat(propChange.isFull()).isTrue();
                        assertThat((Object) propChange.getPath()).isEqualTo(newPointer(""));
                        assertThat(propChange.getValue().get().asString()).isEqualTo("lux");

                        latch.countDown();
                    }
                });

        startConsumingChanges();

        // create the thing with attributes
        client.twin().forId(thingId).putFeature(FEATURE_2).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);

        awaitLatchTrue(latch);
    }

    private String getThingFromRest(final ThingId thingId) {
        try {
            final io.restassured.response.Response response =
                    getThing(2, thingId).withHeader(HttpHeader.CONTENT_TYPE.getName(),
                            TestConstants.CONTENT_TYPE_APPLICATION_JSON_UTF8).fire();
            return response.body().asString();
        } catch (final Exception e) {
            LOGGER.error("GETting Thing {} from REST failed.", thingId, e);
            return null;
        }
    }

    private void startConsumingChanges() {
        LOGGER.info("Starting to consume twin() changes..");
        startConsumptionAndWait(clientSubscriber.twin());
        LOGGER.info("Twin() changes should now be received");
    }
}
