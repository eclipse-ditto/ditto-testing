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
package org.eclipse.ditto.testing.system.connectivity;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.assertj.core.api.SoftAssertions;
import org.eclipse.ditto.base.model.json.Jsonifiable;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttribute;
import org.eclipse.ditto.things.model.signals.events.AttributeCreated;
import org.eclipse.ditto.things.model.signals.events.AttributeModified;
import org.eclipse.ditto.things.model.signals.events.ThingCreated;
import org.eclipse.ditto.things.model.signals.events.ThingEvent;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractConnectivityCommandOrderITestCases<C, M>
        extends AbstractConnectivityITCommon<C, M> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnectivityCommandOrderITestCases.class);

    protected AbstractConnectivityCommandOrderITestCases(final ConnectivityFactory cf) {
        super(cf);
    }

    @Test
    @Connections({ConnectionCategory.CONNECTION1, ConnectionCategory.CONNECTION2})
    public void sendMultipleCommandsEnsuringCommandOrder() throws InterruptedException {
        final SoftAssertions softly = new SoftAssertions();
        // Given
        final ThingBuilder.FromScratch thingBuilder = Thing.newBuilder();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(cf.connectionName1))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .forLabel("RESTRICTED")
                .setSubject(connectionSubject(cf.connectionName2))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ)
                .build();
        final String correlationId = createNewCorrelationId();

        final ThingId thingId1 = generateThingId();
        final CreateThing createThing1 = CreateThing.of(thingBuilder.setId(thingId1).build(), policy.toJson(),
                createDittoHeaders(correlationId + "-1-x"));

        final ThingId thingId2 = generateThingId(RANDOM_NAMESPACE_2);
        final CreateThing createThing2 = CreateThing.of(thingBuilder.setId(thingId2).build(), policy.toJson(),
                createDittoHeaders(correlationId + "-2-x"));

        final ThingId thingId3 = generateThingId();
        final CreateThing createThing3 = CreateThing.of(thingBuilder.setId(thingId3).build(), policy.toJson(),
                createDittoHeaders(correlationId + "-3-x"));

        final JsonPointer modAttrPointer = JsonPointer.of("counter");

        final ModifyAttribute createAttribute1 = ModifyAttribute.of(thingId1, modAttrPointer, JsonValue.of(0),
                createDittoHeaders(correlationId + "-1-0"));
        final ModifyAttribute createAttribute2 = ModifyAttribute.of(thingId2, modAttrPointer, JsonValue.of(0),
                createDittoHeaders(correlationId + "-2-0"));
        final ModifyAttribute createAttribute3 = ModifyAttribute.of(thingId3, modAttrPointer, JsonValue.of(0),
                createDittoHeaders(correlationId + "-3-0"));

        // When
        final C eventConsumer = initTargetsConsumer(cf.connectionName2);

        sendSignal(cf.connectionName1, createThing1);
        sendSignal(cf.connectionName1, createThing2);
        sendSignal(cf.connectionName1, createThing3);
        sendSignal(cf.connectionName1, createAttribute1);
        sendSignal(cf.connectionName1, createAttribute2);
        sendSignal(cf.connectionName1, createAttribute3);

        final int updateCount = 10;
        for (int i = 1; i <= updateCount; i++) {
            final ModifyAttribute modifyAttribute1 = ModifyAttribute.of(thingId1, modAttrPointer, JsonValue.of(i),
                    createDittoHeaders(correlationId + "-1-" + i));
            final ModifyAttribute modifyAttribute2 = ModifyAttribute.of(thingId2, modAttrPointer, JsonValue.of(i),
                    createDittoHeaders(correlationId + "-2-" + i));
            final ModifyAttribute modifyAttribute3 = ModifyAttribute.of(thingId3, modAttrPointer, JsonValue.of(i),
                    createDittoHeaders(correlationId + "-3-" + i));

            sendSignal(cf.connectionName1, modifyAttribute1);
            sendSignal(cf.connectionName1, modifyAttribute2);
            sendSignal(cf.connectionName1, modifyAttribute3);
        }

        final int expectedMessageCount = 6 + (updateCount * 3);
        final CountDownLatch latch = new CountDownLatch(expectedMessageCount);
        final AtomicInteger counter1 = new AtomicInteger(0);
        final AtomicInteger counter2 = new AtomicInteger(0);
        final AtomicInteger counter3 = new AtomicInteger(0);

        for (int i = 0; i < expectedMessageCount; i++) {
            final M eventMessage = consumeFromTarget(cf.connectionName2, eventConsumer);
            softly.assertThat(eventMessage).withFailMessage("Did not receive an event in iteration " + i)
                    .isNotNull();

            final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(eventMessage);
            final Adaptable eventAdaptable1 = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
            final Jsonifiable<JsonObject> event = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable1);

            if (event instanceof final ThingEvent thingEvent) {
                final long actualRevision = thingEvent.getRevision();

                final int expectedRevisionCounter;
                final ThingId thingId = thingEvent.getEntityId();
                if (thingId.equals(thingId1)) {
                    expectedRevisionCounter = counter1.incrementAndGet();
                } else if (thingId.equals(thingId2)) {
                    expectedRevisionCounter = counter2.incrementAndGet();
                } else if (thingId.equals(thingId3)) {
                    expectedRevisionCounter = counter3.incrementAndGet();
                } else {
                    LOGGER.error("Received event for unexpected thingId <{}>", thingId);
                    fail("Received event for unexpected thingId <" + thingId + ">");
                    break;
                }

                if (thingEvent instanceof ThingCreated) {
                    LOGGER.info("it <{}>: Checking that actual revision <{}> is expected <{}> for created Thing with " +
                                    "ID <{}> and correlation ID <{}>",
                            i, actualRevision, expectedRevisionCounter, thingId,
                            thingEvent.getDittoHeaders().getCorrelationId().orElse(""));
                    softly.assertThat(actualRevision)
                            .withFailMessage(
                                    "Expected actual revision <%d> to equal expected revision <%d> for Thing <%s>",
                                    actualRevision, expectedRevisionCounter, thingId)
                            .isEqualTo(expectedRevisionCounter);
                    latch.countDown();
                } else if (thingEvent instanceof AttributeCreated) {
                    final AttributeCreated ac = (AttributeCreated) event;
                    final int actualCounter = ac.getAttributeValue().asInt();

                    LOGGER.info("it <{}>: Checking that actualCounter <{}> eq expected <{}> and " +
                                    "actual revision <{}> eq expected <{}> for ThingID <{}> and correlation ID <{}>",
                            i, actualCounter, expectedRevisionCounter - 2, actualRevision, expectedRevisionCounter,
                            thingId, ac.getDittoHeaders().getCorrelationId().orElse(""));
                    softly.assertThat(actualCounter)
                            .withFailMessage(
                                    "Expected actual counter <%d> to equal expected counter <%d> for Thing <%s>",
                                    actualCounter, 0, thingId)
                            .isZero();
                    softly.assertThat(actualRevision)
                            .withFailMessage(
                                    "Expected actual revision <%d> to equal expected revision <%d> for Thing <%s>",
                                    actualRevision, expectedRevisionCounter, thingId)
                            .isEqualTo(expectedRevisionCounter);
                    latch.countDown();
                } else if (thingEvent instanceof AttributeModified) {
                    final AttributeModified am = (AttributeModified) event;
                    final int actualCounter = am.getAttributeValue().asInt();

                    LOGGER.info("it <{}>: Checking that actualCounter <{}> eq expected <{}> and " +
                                    "actual revision <{}> eq expected <{}> for ThingID <{}> and correlation ID <{}>",
                            i, actualCounter, expectedRevisionCounter - 2, actualRevision, expectedRevisionCounter,
                            thingId, am.getDittoHeaders().getCorrelationId().orElse(""));
                    softly.assertThat(actualCounter)
                            .withFailMessage(
                                    "Expected actual counter <%d> to equal expected counter <%d> for Thing <%s>",
                                    actualCounter, expectedRevisionCounter - 2, thingId)
                            .isEqualTo(expectedRevisionCounter - 2);
                    softly.assertThat(actualRevision)
                            .withFailMessage(
                                    "Expected actual revision <%d> to equal expected revision <%d> for Thing <%s>",
                                    actualRevision, expectedRevisionCounter, thingId)
                            .isEqualTo(expectedRevisionCounter);
                    latch.countDown();
                }
            }
        }

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        softly.assertAll();
    }

}
