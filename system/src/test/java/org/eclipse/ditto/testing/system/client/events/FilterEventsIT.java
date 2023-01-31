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

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.changes.ChangeAction;
import org.eclipse.ditto.client.options.Options;
import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.testing.system.client.util.ThingFactory;
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
 * Verifies if Events are received with the client when triggered via REST applying {@code namespaces} and RQL {@code
 * filter} when staring the client consumption.
 */
public class FilterEventsIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterEventsIT.class);

    private static final String ATTRIBUTE_NAME = "someAttributeName";
    private static final String ATTRIBUTE_PATH = "/someAttributeName";

    private static String interestingNamespace;
    private static String anotherNamespace;

    private AuthClient user;
    private DittoClient dittoClient;

    @Before
    public void setUp() {
        LOGGER.info("-----> Running Test: {}", name.getMethodName());

        interestingNamespace = serviceEnv.getDefaultNamespaceName();
        anotherNamespace = serviceEnv.getSecondaryNamespaceName();
        user = serviceEnv.getDefaultTestingContext().getOAuthClient();
        dittoClient = newDittoClient(user);
    }

    @After
    public void tearDown() {
        shutdownClient(dittoClient);
    }

    @Test
    public void expectStartConsumptionFailsWithMalformedRqlFilter() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);

        dittoClient.twin().startConsumption(
                Options.Consumption.filter("le(attributes/foo,42")
        ).whenComplete((aVoid, throwable) -> {
            if (throwable == null) {
                LOGGER.error("Using an invalid RQL filter should not have been successful");
                latch.countDown();
            }
        });

        awaitLatchFalse(latch);
    }

    @Test
    public void receiveThingCreatedInCertainNamespace() throws Exception {
        final CountDownLatch latch = new CountDownLatch(2);
        final CountDownLatch negativeLatch = new CountDownLatch(1);

        final ThingId thingId1 = ThingId.of(interestingNamespace + ":" + UUID.randomUUID());
        final Thing thing1 = ThingFactory.newThing(thingId1);

        final ThingId thingId2 = ThingId.of(anotherNamespace + ":" + UUID.randomUUID());
        final Thing thing2 = ThingFactory.newThing(thingId2);

        final ThingId thingId3 = ThingId.of(interestingNamespace + ":" + UUID.randomUUID());
        final Thing thing3 = ThingFactory.newThing(thingId3);

        // register for all IDs:
        dittoClient.twin().registerForThingChanges(UUID.randomUUID().toString(), e -> {
            assertThat(e).isNotNull();
            assertThat(e.getAction()).isEqualTo(ChangeAction.CREATED);
            latch.countDown();
        });
        // only register for thingId2:
        dittoClient.twin().forId(thingId2).registerForThingChanges(UUID.randomUUID().toString(), e -> {
            negativeLatch.countDown();
        });

        dittoClient.twin().startConsumption(
                Options.Consumption.namespaces(interestingNamespace)
        ).whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            putThing(2, thing1, JsonSchemaVersion.V_2)
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire();
            putThing(2, thing2, JsonSchemaVersion.V_2)
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire();
            putThing(2, thing3, JsonSchemaVersion.V_2)
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire();
        });

        awaitLatchTrue(latch);
        awaitLatchFalse(negativeLatch);
    }

    @Test
    public void receiveThingDeletedInCertainNamespace() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch negativeLatch = new CountDownLatch(1);

        final ThingId thingId1 = ThingId.of(interestingNamespace + ":" + UUID.randomUUID());
        final Thing thing1 = ThingFactory.newThing(thingId1);

        final ThingId thingId2 = ThingId.of(anotherNamespace + ":" + UUID.randomUUID());
        final Thing thing2 = ThingFactory.newThing(thingId2);

        dittoClient.twin().registerForThingChanges(UUID.randomUUID().toString(), e -> {
            if (ChangeAction.DELETED.equals(e.getAction())) {
                assertThat(e).isNotNull();
                if (e.getEntityId().toString().equalsIgnoreCase(thingId1.toString())) {
                    latch.countDown();
                } else {
                    negativeLatch.countDown();
                }
            }
        });

        dittoClient.twin().startConsumption(
                Options.Consumption.namespaces(interestingNamespace)
        ).whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            putThing(2, thing1, JsonSchemaVersion.V_2)
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire();

            putThing(2, thing2, JsonSchemaVersion.V_2)
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire();

            deleteThing(2, thingId1)
                    .expectingHttpStatus(HttpStatus.NO_CONTENT)
                    .fire();
            deleteThing(2, thingId2)
                    .expectingHttpStatus(HttpStatus.NO_CONTENT)
                    .fire();
        });

        awaitLatchTrue(latch);
        awaitLatchFalse(negativeLatch);
    }

    @Test
    public void receiveAttributeModifiedWithRQLFilter() throws Exception {

        final Random random = new Random();
        final int randomCounter = random.nextInt(20) + 5;
        final int halfCounter = randomCounter / 2;

        final CountDownLatch latch = new CountDownLatch(halfCounter);
        final CountDownLatch latchFull = new CountDownLatch(randomCounter);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId)
                .setAttribute(newPointer(ATTRIBUTE_NAME), JsonValue.of(0));

        dittoClient.twin()
                .forId(thingId)
                .registerForAttributeChanges(UUID.randomUUID().toString(), newPointer(ATTRIBUTE_NAME),
                        attrChange -> {
                            if (attrChange.getAction().equals(ChangeAction.UPDATED)) {
                                assertThat(attrChange).isNotNull();
                                assertThat(attrChange.getPath().toString()).isEqualTo(ATTRIBUTE_PATH);
                                assertThat((CharSequence) attrChange.getEntityId()).isEqualTo(thingId);
                                LOGGER.info("Received attribute changed to: {}", attrChange.getValue().get());
                                latch.countDown();
                            }
                        });

        LOGGER.info("Expecting <{}> changes, changes done are: <{}>", halfCounter, randomCounter);

        // only consume events up until the counter reaches "halfCounter":
        dittoClient.twin().startConsumption(
                Options.Consumption.filter("le(attributes/" + ATTRIBUTE_NAME + "," + halfCounter + ")")
        ).whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            putThing(2, thing, JsonSchemaVersion.V_2)
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire();

            for (int i = 1; i <= randomCounter; i++) {
                putAttribute(2, thingId, ATTRIBUTE_NAME, Integer.toString(i))
                        .expectingHttpStatus(HttpStatus.NO_CONTENT)
                        .fire();
            }
        });

        awaitLatchTrue(latch);
        awaitLatchFalse(latchFull);
    }

    @Test
    public void receiveFeaturesModifiedInCertainNamespaceAndWithRQLfilter() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch negativeLatch = new CountDownLatch(1);

        final ThingId thingId1 = ThingId.of(interestingNamespace + ":" + UUID.randomUUID());
        final Thing thing1 = ThingFactory.newThing(thingId1);
        final Features features1 = ThingsModelFactory.newFeatures(ThingFactory.newFeatures("feature1", "feature2"));

        final ThingId thingId2 = ThingId.of(interestingNamespace + ":" + UUID.randomUUID());
        final Thing thing2 = ThingFactory.newThing(thingId2);
        final Features features2 = ThingsModelFactory.newFeatures(ThingFactory.newFeatures("featureX", "featureZ"));

        final ThingId thingId3 = ThingId.of(anotherNamespace + ":" + UUID.randomUUID());
        final Thing thing3 = ThingFactory.newThing(thingId3);
        final Features features3 = ThingsModelFactory.newFeatures(ThingFactory.newFeatures("feature1"));

        dittoClient.twin().registerForFeaturesChanges(UUID.randomUUID().toString(), change -> {
            LOGGER.info("Received Change: '{}'", change);
            if (change.getEntityId().toString().equalsIgnoreCase(thingId1.toString())) {
                latch.countDown();
            } else {
                negativeLatch.countDown();
            }
        });

        dittoClient.twin().startConsumption(
                Options.Consumption.namespaces(interestingNamespace),
                Options.Consumption.filter("exists(features/feature1)")
        ).whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                LOGGER.error("Error in Test", throwable);
            }

            putThing(2, thing1, JsonSchemaVersion.V_2)
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire();
            putThing(2, thing2, JsonSchemaVersion.V_2)
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire();
            putThing(2, thing3, JsonSchemaVersion.V_2)
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire();

            putFeatures(2, thingId1, features1.toJsonString(JsonSchemaVersion.V_2))
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire();

            putFeatures(2, thingId2, features2.toJsonString(JsonSchemaVersion.V_2))
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire();

            putFeatures(2, thingId3, features3.toJsonString(JsonSchemaVersion.V_2))
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire();
        });

        awaitLatchTrue(latch);
        awaitLatchFalse(negativeLatch);
    }


    @Test
    public void receiveAttributeModifiedWithRQLFilterOnEnrichedData() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId)
                .setAttribute(newPointer("location"), JsonValue.of("kitchen"))
                .setAttribute(newPointer("counter"), JsonValue.of(0));

        dittoClient.twin()
                .forId(thingId)
                .registerForAttributeChanges(UUID.randomUUID().toString(), newPointer("counter"),
                        attrChange -> {
                            if (attrChange.getAction().equals(ChangeAction.UPDATED)) {
                                assertThat(attrChange).isNotNull();
                                assertThat(attrChange.getPath().toString()).isEqualTo("/counter");
                                assertThat(attrChange.getExtra()).isPresent();
                                assertThat(attrChange.getExtra().get().getValue("attributes/location"))
                                        .contains(JsonValue.of("kitchen"));
                                assertThat((CharSequence) attrChange.getEntityId()).isEqualTo(thingId);
                                LOGGER.info("Received attribute changed to: {}", attrChange.getValue().get());
                                latch.countDown();
                            }
                        });

        // only consume events up until the counter reaches "halfCounter":
        dittoClient.twin().startConsumption(
                Options.Consumption.extraFields(JsonFieldSelector.newInstance("attributes/location")),
                Options.Consumption.filter(
                        "and(eq(attributes/location,\"kitchen\"),eq(attributes/counter,42))")
        ).whenComplete((aVoid, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                            .expectingHttpStatus(HttpStatus.CREATED)
                            .fire();

                    putAttribute(TestConstants.API_V_2, thingId, "counter", "42")
                            .expectingHttpStatus(HttpStatus.NO_CONTENT)
                            .fire();
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void filterLifecycleEventsOnlyCreated() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);
        final CountDownLatch negativeLatch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId)
                .setAttribute(newPointer("foo"), JsonValue.of("bar"));

        dittoClient.twin()
                .forId(thingId)
                .registerForThingChanges(UUID.randomUUID().toString(), thingChange -> {
                    if (thingChange.getAction().equals(ChangeAction.CREATED)) {
                        latch.countDown();
                    } else {
                        negativeLatch.countDown();
                        softly.fail("Filter for <and(eq(attributes/foo,'bar'),eq(topic:action,'created'))> " +
                                "must only deliver thing created changes");
                    }
                });

        dittoClient.twin().startConsumption(
                Options.Consumption.filter("and(eq(attributes/foo,'bar'),eq(topic:action,'created'))")
        ).whenComplete((aVoid, throwable) -> {
                if (throwable != null) {
                    LOGGER.error("Error in Test", throwable);
                }

                putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                        .expectingHttpStatus(HttpStatus.CREATED)
                        .fire();

                putAttribute(TestConstants.API_V_2, thingId, "foo", "\"bar\"")
                        .expectingHttpStatus(HttpStatus.NO_CONTENT)
                        .fire();

                deleteThing(TestConstants.API_V_2, thingId);
        });

        awaitLatchTrue(latch);
        awaitLatchFalse(negativeLatch);
    }
    
    @Test
    public void filterLifecycleEventsOnlyModifiedOrMerged() throws Exception {

        final CountDownLatch modifiedLatch = new CountDownLatch(1);
        final CountDownLatch mergedLatch = new CountDownLatch(1);
        final CountDownLatch negativeLatch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId)
                .setAttribute(newPointer("foo"), JsonValue.of("bar"));

        dittoClient.twin()
                .forId(thingId)
                .registerForThingChanges(UUID.randomUUID().toString(), thingChange -> {
                    if (thingChange.getAction().equals(ChangeAction.UPDATED)) {
                        modifiedLatch.countDown();
                    } else if (thingChange.getAction().equals(ChangeAction.MERGED)) {
                        mergedLatch.countDown();
                    } else {
                        negativeLatch.countDown();
                        softly.fail("Filter for <in(topic:action,'modified','merged')> must only deliver " +
                                "thing updated or merged changes");
                    }
                });

        dittoClient.twin().startConsumption(
                Options.Consumption.filter("in(topic:action,'modified','merged')")
        ).whenComplete((aVoid, throwable) -> {
                if (throwable != null) {
                    LOGGER.error("Error in Test", throwable);
                }

                putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                        .expectingHttpStatus(HttpStatus.CREATED)
                        .fire();

                putAttribute(TestConstants.API_V_2, thingId, "foo", "\"bar\"")
                        .expectingHttpStatus(HttpStatus.NO_CONTENT)
                        .fire();

                patchThing(thingId, JsonPointer.of("/attributes/foo"), JsonValue.of("no-bar"))
                        .expectingHttpStatus(HttpStatus.NO_CONTENT)
                        .fire();

                deleteThing(TestConstants.API_V_2, thingId);
        });

        awaitLatchTrue(modifiedLatch);
        awaitLatchTrue(mergedLatch);
        awaitLatchFalse(negativeLatch);
    }
}
