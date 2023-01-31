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

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.json.JsonFactory.newPointer;
import static org.eclipse.ditto.json.JsonFactory.newValue;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.changes.Change;
import org.eclipse.ditto.client.changes.ChangeAction;
import org.eclipse.ditto.client.management.ThingHandle;
import org.eclipse.ditto.client.options.Options;
import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.testing.system.client.util.ThingFactory;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.Features;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies if Events are received with the client when triggered via REST.
 */
public class RegisterForEventsIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(RegisterForEventsIT.class);

    private static final String ATTRIBUTE_NAME = "someAttributeName";
    private static final String ATTRIBUTE_PATH = "/someAttributeName";

    private AuthClient user;
    private DittoClient dittoClient;

    @Before
    public void setUp() {
        LOGGER.info("-----> Running Test: {}", name.getMethodName());

        user = serviceEnv.getDefaultTestingContext().getOAuthClient();
        dittoClient = newDittoClient(user);
    }

    @After
    public void tearDown() {
        shutdownClient(dittoClient);
    }

    @Test
    public void receiveThingCreated() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);

        dittoClient.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(), e ->
        {
            assertThat(e).isNotNull();
            assertThat(e.getAction()).isEqualTo(ChangeAction.CREATED);
            assertThat(e.getThing().flatMap(Thing::getEntityId)).hasValue(thingId);
            latch.countDown();
        });

        dittoClient.twin().startConsumption()
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    putThing(2, thing, JsonSchemaVersion.V_2)
                            .expectingHttpStatus(HttpStatus.CREATED)
                            .fire();
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void receiveThingDeleted() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);

        dittoClient.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(), change ->
        {
            if (change.getEntityId().equals(thingId) && ChangeAction.DELETED.equals(change.getAction())) {
                assertThat(change).isNotNull();
                assertThat(change.getAction()).isEqualTo(ChangeAction.DELETED);
                assertThat(change.getEntityId().toString()).isEqualTo(thingId.toString());
                latch.countDown();
            }
        });

        dittoClient.twin().startConsumption()
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    putThing(2, thing, JsonSchemaVersion.V_2)
                            .expectingHttpStatus(HttpStatus.CREATED)
                            .fire();

                    deleteThing(2, thingId)
                            .expectingHttpStatus(HttpStatus.NO_CONTENT)
                            .fire();
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void receiveAttributeModified() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId)
                .setAttribute(newPointer(ATTRIBUTE_NAME), newValue(UUID.randomUUID().toString()));
        final String attributeValue = "someValue";

        final ThingHandle thing1 = dittoClient.twin().forId(thingId);
        thing1.registerForAttributeChanges(UUID.randomUUID().toString(), newPointer(ATTRIBUTE_NAME),
                thingAttributeChange -> {
                    assertThat(thingAttributeChange).isNotNull();
                    assertThat(thingAttributeChange.getAction()).isEqualTo(ChangeAction.UPDATED);
                    assertThat(thingAttributeChange.getPath().toString()).isEqualTo(ATTRIBUTE_PATH);
                    assertThat(thingAttributeChange.getValue().get().asString()).isEqualTo(attributeValue);
                    assertThat((CharSequence) thingAttributeChange.getEntityId()).isEqualTo(thingId);
                    latch.countDown();
                });

        dittoClient.twin().startConsumption()
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    putThing(2, thing, JsonSchemaVersion.V_2)
                            .expectingHttpStatus(HttpStatus.CREATED)
                            .fire();

                    putAttribute(2, thingId, ATTRIBUTE_NAME, newValue(attributeValue).toString())
                            .withHeader(HttpHeader.CONTENT_TYPE.getName(),
                                    TestConstants.CONTENT_TYPE_APPLICATION_JSON_UTF8)
                            .expectingHttpStatus(HttpStatus.NO_CONTENT)
                            .fire();
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void receiveAttributeDeleted() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId)
                .setAttribute(newPointer(ATTRIBUTE_NAME), newValue(true));

        dittoClient.twin().forId(thingId).registerForAttributeChanges(UUID.randomUUID().toString(),
                newPointer(ATTRIBUTE_NAME), change -> {
                    if (ChangeAction.DELETED.equals(change.getAction())) {
                        assertThat((Object) change.getPath().toString()).isEqualTo(ATTRIBUTE_PATH);
                        assertThat((CharSequence) change.getEntityId()).isEqualTo(thingId);
                        latch.countDown();
                    }
                });

        dittoClient.twin().startConsumption()
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    putThing(2, thing, JsonSchemaVersion.V_2)
                            .expectingHttpStatus(HttpStatus.CREATED)
                            .fire();
                    deleteAttribute(2, thingId, ATTRIBUTE_NAME)
                            .expectingHttpStatus(HttpStatus.NO_CONTENT)
                            .fire();
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void receiveAttributesModified() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);
        final JsonValue attributes = ThingsModelFactory.newAttributesBuilder().set(ATTRIBUTE_NAME, true).build();

        putThing(2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        dittoClient.twin().forId(thingId).registerForAttributesChanges(UUID.randomUUID().toString(),
                createModifiedChangeConsumer(latch, thingId, JsonPointer.empty(), attributes));

        dittoClient.twin().startConsumption()
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    putAttributes(2, thingId, attributes.toString())
                            .withHeader(HttpHeader.CONTENT_TYPE.getName(),
                                    TestConstants.CONTENT_TYPE_APPLICATION_JSON_UTF8)
                            .expectingHttpStatus(HttpStatus.CREATED)
                            .fire();
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void receiveAttributesDeleted() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId)
                .setAttribute(newPointer(ATTRIBUTE_NAME), newValue(true));

        dittoClient.twin().forId(thingId).registerForAttributesChanges(UUID.randomUUID().toString(),
                createDeletedChangeConsumer(latch, thingId, JsonPointer.empty()));

        dittoClient.twin().startConsumption()
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    putThing(2, thing, JsonSchemaVersion.V_2)
                            .expectingHttpStatus(HttpStatus.CREATED)
                            .fire();
                    deleteAttributes(2, thingId)
                            .expectingHttpStatus(HttpStatus.NO_CONTENT)
                            .fire();
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void receiveFeaturesModified() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);
        final Features features = ThingsModelFactory.newFeatures(ThingFactory.newFeatures("feature1", "feature2"));

        dittoClient.twin().forId(thingId).registerForFeaturesChanges(UUID.randomUUID().toString(),
                createModifiedChangeConsumer(latch, thingId, JsonPointer.empty(), features.toJson()));

        dittoClient.twin().forId(thingId).registerForFeaturesChanges(UUID.randomUUID().toString(), change -> {
            LOGGER.info("Received Change: '{}'", change);
            latch.countDown();
        });

        dittoClient.twin().startConsumption()
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    putThing(2, thing, JsonSchemaVersion.V_2)
                            .expectingHttpStatus(HttpStatus.CREATED)
                            .fire();

                    putFeatures(2, thingId, features.toJsonString(JsonSchemaVersion.V_2))
                            .expectingHttpStatus(HttpStatus.CREATED)
                            .fire();
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void receiveFeaturesDeleted() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId)
                .setFeatures(ThingFactory.newFeatures("feature1", "feature2"));

        putThing(2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        dittoClient.twin().forId(thingId).registerForFeaturesChanges(UUID.randomUUID().toString(),
                createDeletedChangeConsumer(latch, thingId, JsonPointer.empty()));

        dittoClient.twin().startConsumption()
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    deleteFeatures(2, thingId)
                            .expectingHttpStatus(HttpStatus.NO_CONTENT)
                            .fire();
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void receiveFeaturesModifiedViaMultipleRegistrations() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(3);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);
        final Features features = ThingsModelFactory.newFeatures(ThingFactory.newFeatures("feature1", "feature2"));

        dittoClient.twin().forId(thingId).registerForFeaturesChanges(UUID.randomUUID().toString(), change -> {
            LOGGER.info("Received Change: '{}' for all", change);
            latch.countDown();
        });

        dittoClient.twin()
                .forId(thingId)
                .registerForFeatureChanges(UUID.randomUUID().toString(), "feature1", change -> {
                    LOGGER.info("Received Change: '{}' for feature1", change);
                    latch.countDown();
                });

        dittoClient.twin()
                .forId(thingId)
                .registerForFeatureChanges(UUID.randomUUID().toString(), "feature2", change -> {
                    LOGGER.info("Received Change: '{}' for feature2", change);
                    latch.countDown();
                });

        dittoClient.twin().startConsumption()
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    putThing(2, thing, JsonSchemaVersion.V_2)
                            .expectingHttpStatus(HttpStatus.CREATED)
                            .fire();

                    putFeatures(2, thingId, features.toJsonString(JsonSchemaVersion.V_2))
                            .expectingHttpStatus(HttpStatus.CREATED)
                            .fire();
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void receiveFeaturesModifiedWithEnrichedAttributes() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(2);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setAttribute(JsonPointer.of("hello"), JsonValue.of("World"))
                .build();
        final Features features = Features.newBuilder()
                .set(Feature.newBuilder().withId("feature1").build().setProperty("test", "foo"))
                .set(Feature.newBuilder().withId("feature2").build().setProperty("test", "bar"))
                .build();

        dittoClient.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(), change -> {
            LOGGER.info("Received Change: '{}'", change);
            if (change.getPath().equals(JsonPointer.of("/features"))) {
                assertThat(change.getExtra()).isPresent();
                final JsonObject extra = change.getExtra().get();
                assertThat(extra.getValue("attributes/hello")).contains(JsonValue.of("World"));
                assertThat(extra.getValue("features/feature1/properties/test")).contains(JsonValue.of("foo"));
                assertThat(extra.getValue("features/feature2/properties/test")).contains(JsonValue.of("bar"));
                latch.countDown();
            } else if (change.getPath().equals(JsonPointer.of("/features/feature1/properties/test"))) {
                assertThat(change.getExtra()).isPresent();
                final JsonObject extra = change.getExtra().get();
                assertThat(extra.getValue("attributes/hello")).contains(JsonValue.of("World"));
                assertThat(extra.getValue("features/feature1/properties/test")).contains(JsonValue.of("bum"));
                assertThat(extra.getValue("features/feature2/properties/test")).isEmpty();
                latch.countDown();
            }
        });

        dittoClient.twin()
                .startConsumption(Options.Consumption.extraFields(
                        /*
                         * The fn:lower() function is used just to ensure that this works.
                         * It wouldn't actually have any effect since the feature IDs are actually already lower case.
                         */
                        JsonFieldSelector.newInstance("attributes",
                                "/features/{{feature:id | fn:lower() }}/properties/test")))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    putFeatures(TestConstants.API_V_2, thingId, features.toJsonString(JsonSchemaVersion.V_2))
                            .expectingHttpStatus(HttpStatus.CREATED)
                            .fire();

                    patchThing(thingId, JsonPointer.of("/features/feature1/properties/test"), JsonValue.of("bum"))
                            .expectingHttpStatus(HttpStatus.NO_CONTENT)
                            .fire();
                });

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        awaitLatchTrue(latch);
    }

    @Test
    public void receiveThingDeletedWithEnrichedAttributesWithoutCache() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setAttribute(JsonPointer.of("hello"), JsonValue.of("World"))
                .build();

        dittoClient.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(), change -> {
            LOGGER.info("Received Change: '{}'", change);
            if (change.getAction().equals(ChangeAction.DELETED) && change.isFull()) {
                assertThat(change.getExtra()).isPresent();
                final JsonObject extra = change.getExtra().get();
                assertThat(extra.getValue("attributes/hello")).contains(JsonValue.of("World"));
                latch.countDown();
            }
        });

        /*
         Create the thing before start consumption in order to be sure that it's not in the cache and signal enrichment
         has to do the roundtrip.
         */
        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        dittoClient.twin()
                .startConsumption(Options.Consumption.extraFields(JsonFieldSelector.newInstance("attributes")))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    deleteThing(TestConstants.API_V_2, thingId)
                            .expectingHttpStatus(HttpStatus.NO_CONTENT)
                            .fire();
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void receiveThingDeletedWithEnrichedAttributesWithCache() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setAttribute(JsonPointer.of("hello"), JsonValue.of("World"))
                .build();

        dittoClient.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(), change -> {
            LOGGER.info("Received Change: '{}'", change);
            if (change.getAction().equals(ChangeAction.DELETED) && change.isFull()) {
                assertThat(change.getExtra()).isPresent();
                final JsonObject extra = change.getExtra().get();
                assertThat(extra.getValue("attributes/hello")).contains(JsonValue.of("World"));
                latch.countDown();
            }
        });

        dittoClient.twin()
                .startConsumption(Options.Consumption.extraFields(JsonFieldSelector.newInstance("attributes")))
                .whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }
                }).toCompletableFuture().join();

        /*
         Create the thing after start consumption in order to be sure that it's in the cache and signal enrichment can
         use the cache.
         */
        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        awaitLatchTrue(latch);
    }

    private <C extends Change> Consumer<C> createModifiedChangeConsumer(final CountDownLatch latch,
            final ThingId thingId, final JsonPointer path, final JsonValue value) {
        return change -> {
            LOGGER.info("Received Change '{}'", change);

            assertThat(change).isNotNull();

            if (!(change.getAction().equals(ChangeAction.CREATED) || change.getAction().equals(ChangeAction.UPDATED))) {
                // we are only interested in modification changes
                return;
            }

            assertThat((CharSequence) change.getEntityId()).isEqualTo(thingId);
            assertThat((Object) change.getPath()).isEqualTo(path);
            assertThat(change.getValue()).contains(value);

            latch.countDown();
        };
    }

    private <C extends Change> Consumer<C> createDeletedChangeConsumer(final CountDownLatch latch,
            final ThingId thingId,
            final JsonPointer path) {
        return change -> {
            LOGGER.info("Received Change '{}'", change);

            assertThat(change).isNotNull();

            if (!change.getAction().equals(ChangeAction.DELETED)) {
                // we are only interested in deletion changes
                return;
            }

            assertThat((CharSequence) change.getEntityId()).isEqualTo(thingId);
            assertThat((Object) change.getPath()).isEqualTo(path);

            latch.countDown();
        };
    }
}
