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

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.FieldType;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.options.Option;
import org.eclipse.ditto.client.options.Options;
import org.eclipse.ditto.client.twin.Twin;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonFieldDefinition;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonObjectBuilder;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.Permissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.testing.system.client.util.ThingFactory;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.AttributesModelFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingIdInvalidException;
import org.eclipse.ditto.things.model.ThingTooLargeException;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.exceptions.ThingConditionFailedException;
import org.eclipse.ditto.things.model.signals.commands.exceptions.ThingConditionInvalidException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for managing {@link org.eclipse.ditto.things.model.Thing}s.
 */
public class ManageThingsIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ManageThingsIT.class);

    private static final JsonPointer ATTRIBUTE_POINTER = JsonFactory.newPointer("foo");
    private static final JsonValue ATTRIBUTE_VALUE = JsonFactory.newValue("bar");
    private static final JsonValue ATTRIBUTE_VALUE_2 = JsonFactory.newValue("biz");

    private AuthClient user;
    private AuthClient subscriber;
    private DittoClient dittoClient;
    private DittoClient dittoClientV2;
    private DittoClient dittoClientSubscriber;
    private DittoClient dittoClientUser2;

    @Before
    public void setUp() {
        LOGGER.info("-----> Running Test: {}", name.getMethodName());

        user = serviceEnv.getDefaultTestingContext().getOAuthClient();
        subscriber = serviceEnv.getTestingContext2().getOAuthClient();

        dittoClient = newDittoClientV2(user);
        dittoClientV2 = newDittoClientV2(user);
        dittoClientSubscriber = newDittoClientV2(subscriber);
        dittoClientUser2 = newDittoClientV2(subscriber);
    }

    @After
    public void tearDown() {
        shutdownClient(dittoClient);
        shutdownClient(dittoClientSubscriber);
    }

    @Test
    public void receiveResponseWhenCreatingAThing() throws InterruptedException, ExecutionException, TimeoutException {

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Policy policy = PoliciesModelFactory.newPolicy(TestConstants.Policy.DEFAULT_POLICY);

        dittoClient.twin()
                .create(thingId, policy)
                .whenComplete((thing, throwable) -> {
                    assertThat(thing)
                            .overridingErrorMessage("Thing is null")
                            .isNotNull();
                    assertThat(thing.getEntityId())
                            .overridingErrorMessage("Ids are not equal")
                            .hasValue(thingId);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void receiveSelectFieldsWhenRetrievingAThing() throws Exception {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Policy policy = PoliciesModelFactory.newPolicy(TestConstants.Policy.DEFAULT_POLICY);

        final List<JsonFieldDefinition<?>> fields = List.of(Thing.JsonFields.ID,
                Thing.JsonFields.CREATED,
                Thing.JsonFields.MODIFIED,
                Thing.JsonFields.REVISION);
        final var selectedFields = JsonFactory.newFieldSelector(fields.stream()
                .map(JsonFieldDefinition::getPointer)
                .collect(Collectors.toList()));

        dittoClient.twin()
                .create(thingId, policy)
                .thenCompose(thing -> dittoClient.twin().forId(thingId).retrieve(selectedFields))
                .whenComplete((thing, throwable) -> {
                    assertThat(thing).isNotNull();
                    final var thingJsonObjectBuilder = JsonFactory.newObjectBuilder(thing.toJson(FieldType.all()))
                            .remove(Thing.JsonFields.NAMESPACE); // ignore, field is always contained in JSON

                    // verify result contained only the selected fields, nothing else
                    assertThat(fields.stream()
                            .reduce(thingJsonObjectBuilder, JsonObjectBuilder::remove, JsonObjectBuilder::setAll)
                            .build())
                            .isEmpty();
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void receiveResponseWhenDeletingAThing() throws InterruptedException, ExecutionException, TimeoutException {

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        dittoClient.twin()
                .create(thingId)
                .thenCompose(thing -> dittoClient.twin().delete(thing.getEntityId().get()))
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test(expected = ThingIdInvalidException.class)
    public void receiveErrorWhenCreatingAThingWithInvalidId()
            throws InterruptedException, ExecutionException, TimeoutException {

        dittoClient.twin().create(ThingId.of("invalid"))
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test(expected = ThingIdInvalidException.class)
    public void receiveErrorWhenCreatingAThingWithInvalidBody()
            throws InterruptedException, ExecutionException, TimeoutException {

        dittoClient.twin()
                .create(JsonFactory.newObjectBuilder().set("thingId", "invalid").build())
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void updateThing() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(2);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);

        startConsumptionAndWait(dittoClientSubscriber.twin());

        dittoClientSubscriber.twin().forId(thingId)
                .registerForThingChanges(UUID.randomUUID().toString(), change -> latch.countDown());

        dittoClient.twin().create(thing, policy).thenAccept(
                created -> dittoClient.twin().update(created.setAttribute(ATTRIBUTE_POINTER, ATTRIBUTE_VALUE)));

        awaitLatchTrue(latch);
    }

    @Test
    public void mergeThing() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(3);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(ATTRIBUTE_POINTER, ATTRIBUTE_VALUE)
                .build();

        startConsumptionAndWait(dittoClientSubscriber.twin());

        dittoClientSubscriber.twin().forId(thingId)
                .registerForThingChanges(UUID.randomUUID().toString(), change -> latch.countDown());

        final Policy policy = PoliciesModelFactory.newPolicy(TestConstants.Policy.DEFAULT_POLICY)
                .setSubjectFor("DEFAULT", subscriber.getSubject());

        dittoClientV2.twin().create(thing, policy)
                .thenCompose(created -> dittoClientV2.twin().merge(thingId,
                        created.setAttribute(ATTRIBUTE_POINTER, ATTRIBUTE_VALUE_2)))
                .thenCompose(aVoid -> dittoClientV2.twin().forId(thingId).retrieve())
                .thenAccept(things -> things.getAttributes()
                        .flatMap(attributes -> attributes.getValue(ATTRIBUTE_POINTER))
                        .ifPresent(jsonValue -> {
                            if (jsonValue.asString().equals(ATTRIBUTE_VALUE_2.asString())) {
                                latch.countDown();
                            }
                        }));

        awaitLatchTrue(latch);
    }

    @Test
    public void mergeThingIsDeletedWithNullObject() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(2);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setAttribute(ATTRIBUTE_POINTER, ATTRIBUTE_VALUE)
                .setFeature(ThingsModelFactory.newFeature("aaa"))
                .build();

        startConsumptionAndWait(dittoClientSubscriber.twin());

        dittoClientSubscriber.twin().forId(thingId)
                .registerForThingChanges(UUID.randomUUID().toString(), change -> latch.countDown());

        final Policy policy = PoliciesModelFactory.newPolicy(TestConstants.Policy.DEFAULT_POLICY)
                .setSubjectFor("DEFAULT", subscriber.getSubject());

        dittoClientV2.twin().create(thing, policy)
                .thenCompose(
                        created -> dittoClientV2.twin().merge(thingId, created.setFeatures(null).setAttributes(null))
                                .thenCompose(aVoid -> dittoClientV2.twin().retrieve(thingId))
                                .thenAccept(things -> {
                                    final Thing thing1 = things.get(0);
                                    LOGGER.info("Thing: {}", thing1);
                                    if (thing1.getAttributes().isEmpty()) {
                                        latch.countDown();
                                    }
                                    if (thing1.getFeatures().isEmpty()) {
                                        latch.countDown();
                                    }
                                }));

        awaitLatchTrue(latch);
    }

    @Test
    public void updateThingFailsIfDoesNotExist() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId).build();

        final Twin twin = dittoClient.twin();

        twin.update(thing.setAttribute(ATTRIBUTE_POINTER, ATTRIBUTE_VALUE))
                .whenComplete((aVoid, throwable) -> {
                    if (null != throwable) {
                        LOGGER.info("Received Exception: '{}'", throwable.getMessage());
                        latch.countDown();
                    }
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void mergeThingFailsIfDoesNotExist() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId).build();

        dittoClient.twin().merge(thingId, thing.setAttribute(ATTRIBUTE_POINTER, ATTRIBUTE_VALUE))
                .whenComplete((aVoid, throwable) -> {
                    if (null != throwable) {
                        LOGGER.info("Received Exception: '{}'", throwable.getMessage());
                        latch.countDown();
                    }
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void putThingCreatesThingIfItDoesNotYetExist() throws Exception {

        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final Policy policy = newPolicy(policyId, List.of(user, subscriber), List.of(user));
        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId).build();
        startConsumptionAndWait(dittoClientSubscriber.twin());

        dittoClientSubscriber.twin().forId(thingId)
                .registerForThingChanges(UUID.randomUUID().toString(), change -> latch.countDown());

        final Optional<Thing> optionalWithExpectedThing = dittoClient.twin().put(thing, policy)
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(optionalWithExpectedThing).isPresent();

        awaitLatchTrue(latch);
    }

    @Test
    public void putThingOverwritesThingIfItAlreadyExists() throws Exception {

        final CountDownLatch latch = new CountDownLatch(2);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);

        startConsumptionAndWait(dittoClientSubscriber.twin());

        dittoClientSubscriber.twin().forId(thingId)
                .registerForThingChanges(UUID.randomUUID().toString(), change -> latch.countDown());

        final Optional<Thing> optionalExpectedToBeEmpty =
                dittoClient.twin().create(thing, policy)
                        .thenCompose(created -> dittoClient.twin()
                                .put(created.setAttribute(ATTRIBUTE_POINTER, ATTRIBUTE_VALUE)))
                        .toCompletableFuture()
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(optionalExpectedToBeEmpty).isEmpty();

        awaitLatchTrue(latch);
    }

    @Test
    public void putThingWithOptionExistsFalseFailsIfThingAlreadyExists() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId).build();

        final Option<Boolean> doesNotExist = Options.Modify.exists(false);
        dittoClient.twin().create(thing).thenAccept(
                created -> dittoClient.twin().put(thing.setAttribute(ATTRIBUTE_POINTER, ATTRIBUTE_VALUE), doesNotExist)
                        .whenComplete((aVoid, throwable) -> {
                            if (null != throwable) {
                                LOGGER.info("Received expected Exception: '{}'", throwable.getMessage());
                                latch.countDown();
                            }
                        }));

        awaitLatchTrue(latch);
    }

    @Test
    public void putThingWithOptionExistsTrueFailsIfThingDoesNotYetExist() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId).build();

        final Option<Boolean> exists = Options.Modify.exists(true);
        dittoClient.twin().put(thing, exists)
                .whenComplete((aVoid, throwable) -> {
                    if (null != throwable) {
                        LOGGER.info("Received expected Exception: '{}'", throwable.getMessage());
                        latch.countDown();
                    }
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void createThingFailsIfThingTooLarge() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < DEFAULT_MAX_THING_SIZE; i++) {
            sb.append('a');
        }
        final JsonObject largeAttributes = JsonObject.newBuilder()
                .set("a", sb.toString())
                .build();

        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId)
                .setAttributes(largeAttributes)
                .build();

        final Twin twin = dittoClient.twin();

        twin.create(thing)
                .whenComplete((aVoid, throwable) -> {
                    if (null != throwable) {
                        LOGGER.info("Received expected Exception: '{}'", throwable.getMessage());
                        assertThat(throwable.getCause()).isInstanceOf(ThingTooLargeException.class);
                        latch.countDown();
                    }
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void createThingWithCopiedPolicy() throws InterruptedException, TimeoutException, ExecutionException {

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId).build();
        final Thing thingCopy = ThingsModelFactory.newThingBuilder().setGeneratedId().build();

        putThing(2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final Policy generatedPolicy = dittoClientV2.policies()
                .retrieve(PolicyId.of(thingId.toString()))
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        final Policy policyOfCreatedThing = dittoClientV2.twin()
                .create(thingCopy, Options.Modify.copyPolicyFromThing(thingId))
                .thenCompose(thing1 -> dittoClientV2.policies().retrieve(thing1.getPolicyId().orElseThrow()))
                .toCompletableFuture()
                .join();

        assertThat(policyOfCreatedThing.getEntriesSet()).isEqualTo(generatedPolicy.getEntriesSet());
    }

    @Test
    public void createThingWithCopiedPolicyAndAllowPolicyLockoutHeader() {
        final String randomName = idGenerator().withRandomName();
        final ThingId thingId = ThingId.of(randomName + "-thing");
        final PolicyId policyId = PolicyId.of(randomName + "-policy");
        final Permissions read = PoliciesModelFactory.newPermissions("READ");
        final Policy policyRead = buildPolicyWithSpecialPolicyPermissions(policyId, read);
        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId).build();
        final Option<DittoHeaders> allowPolicyLockoutHeader =
                Options.headers(DittoHeaders.newBuilder().allowPolicyLockout(true).build());

        // create the policy to be copied and retrieve its policy
        putPolicy(policyRead)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        // create a thing with _copyPolicyFrom option
        final Policy policyOfCreatedThing = dittoClient.twin()
                .create(thing, allowPolicyLockoutHeader, Options.Modify.copyPolicy(policyId))
                .thenCompose(createdThing -> dittoClient.policies()
                        .retrieve(createdThing.getPolicyId().orElseThrow()))
                .toCompletableFuture()
                .join();

        // verify entries of copied policy are equal to original entries
        assertThat(policyOfCreatedThing.getEntriesSet()).isEqualTo(policyRead.getEntriesSet());
    }

    @Test
    public void updateThingWithCondition() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final Attributes attributes = AttributesModelFactory.newAttributesBuilder()
                .set("location", "IMB")
                .build();
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId).setAttributes(attributes);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);

        // create thing with attribute to perform conditional update in the next step
        putThingWithPolicy(2, thing, policy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        startConsumptionAndWait(dittoClientSubscriber.twin());

        dittoClientSubscriber.twin().forId(thingId)
                .registerForThingChanges(UUID.randomUUID().toString(), change -> latch.countDown());

        dittoClient.twin().update(thing, Options.condition("eq(attributes/location,\"IMB\")"))
                .toCompletableFuture().join();

        awaitLatchTrue(latch);
    }

    @Test
    public void updateThingWithConditionFails() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final Attributes attributes = AttributesModelFactory.newAttributesBuilder()
                .set("location", "IMB")
                .build();
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId).setAttributes(attributes);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);

        // create thing with attribute to perform conditional update in the next step
        putThingWithPolicy(2, thing, policy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        dittoClient.twin().update(thing, Options.condition("eq(attributes/location,\"Lubu\")"))
                .whenComplete((aVoid, throwable) -> {
                    if (null != throwable) {
                        LOGGER.info("Received expected Exception: '{}'", throwable.getMessage());
                        assertThat(throwable.getCause()).isInstanceOf(ThingConditionFailedException.class);
                        latch.countDown();
                    }
                });

        awaitLatchTrue(latch);
    }

    @Test
    public void updateThingWithInvalidCondition() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final Attributes attributes = AttributesModelFactory.newAttributesBuilder()
                .set("location", "IMB")
                .build();
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId).setAttributes(attributes);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);

        // create thing with attribute to perform conditional update in the next step
        putThingWithPolicy(2, thing, policy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        dittoClient.twin().update(thing, Options.condition("eq(attributes/location,\"IMB\""))
                .whenComplete((aVoid, throwable) -> {
                    if (null != throwable) {
                        LOGGER.info("Received expected Exception: '{}'", throwable.getMessage());
                        assertThat(throwable.getCause()).isInstanceOf(ThingConditionInvalidException.class);
                        latch.countDown();
                    }
                });

        awaitLatchTrue(latch);
    }

    private Policy buildPolicyWithSpecialPolicyPermissions(final PolicyId policyId, final Permissions policyGrants) {
        return Policy.newBuilder(policyId)
                .forLabel(Label.of("restrictedPolicyUser"))
                .setSubject(serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), "READ", "WRITE")
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), policyGrants)
                .forLabel("ADMIN")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), "READ", "WRITE")
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), "READ", "WRITE")
                .build();
    }

}
