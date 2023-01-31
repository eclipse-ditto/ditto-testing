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
package org.eclipse.ditto.testing.system.client.ack;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;

import org.assertj.core.api.SoftAssertions;
import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.acks.AcknowledgementLabelNotUniqueException;
import org.eclipse.ditto.base.model.acks.AcknowledgementRequest;
import org.eclipse.ditto.base.model.acks.DittoAcknowledgementLabel;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.headers.translator.HeaderTranslator;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgement;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgements;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.changes.ChangeAction;
import org.eclipse.ditto.client.management.AcknowledgementsFailedException;
import org.eclipse.ditto.client.messaging.AuthenticationException;
import org.eclipse.ditto.client.options.Options;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.testing.system.client.util.ThingFactory;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests that {@code Acknowledgements} requested via {@code AcknowledgementRequest}s added to the DittoHeaders of
 * modifying ThingCommands can be acknowledged by Ditto clients + the {@code Acknowledgements} response may also be
 * parsed by a Ditto client.
 */
public final class ClientAcknowledgementIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientAcknowledgementIT.class);
    private static final DittoProtocolAdapter DITTO_PROTOCOL_ADAPTER =
            DittoProtocolAdapter.of(HeaderTranslator.empty());


    private static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(6);
    private static final AcknowledgementLabel DECLARED_ACK = AcknowledgementLabel.of(
            serviceEnv.getTestingContext2().getSolution().getUsername() + ":my-custom-ack");
    private static final AcknowledgementLabel REQUESTED_ACK =
            AcknowledgementLabel.of(serviceEnv.getTestingContext2().getSolution().getUsername() + ":my-custom-ack");

    private AuthClient user;
    private AuthClient userSubscriber;

    private DittoClient client;
    private DittoClient clientSubscriber;

    @Before
    public void setUp() {
        LOGGER.info("-----> Running Test: {}", name.getMethodName());

        user = serviceEnv.getDefaultTestingContext().getOAuthClient();
        userSubscriber = serviceEnv.getTestingContext2().getOAuthClient();

        client = newDittoClient(user);
        LOGGER.info("Created dittoClient");
    }

    @After
    public void tearDown() {
        shutdownClient(client);
        shutdownClient(clientSubscriber);
    }

    @Test
    public void createThingWithAcknowledgementRequests() throws Exception {
        clientSubscriber =
                newDittoClientV2(userSubscriber, Collections.singleton(DECLARED_ACK), Collections.emptyList());
        LOGGER.info("Created dittoClientSubscriber");
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final Thing thing = ThingFactory.newThing(thingId).setPolicyId(policyId);
        final Policy policy = newPolicy(policyId, user, userSubscriber);

        clientSubscriber.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(),
                thingChange -> {
                    LOGGER.info("received ThingChange {}", thingChange);

                    if (ChangeAction.CREATED.equals(thingChange.getAction())) {
                        assertThat(thingChange.getThing().flatMap(Thing::getEntityId)).hasValue(thingId);
                        assertThat(thingChange.getThing()).hasValue(thing);
                        assertThat(thingChange.getDittoHeaders().getAcknowledgementRequests()).contains(
                                AcknowledgementRequest.of(REQUESTED_ACK));

                        thingChange.handleAcknowledgementRequest(REQUESTED_ACK, ackRequest ->
                                ackRequest.acknowledge(HttpStatus.OK));

                        latch.countDown();
                    }
                });

        startConsumptionAndWait(clientSubscriber.twin());

        client.twin().create(thing, policy, Options.headers(DittoHeaders.newBuilder()
                .acknowledgementRequest(
                        AcknowledgementRequest.of(DittoAcknowledgementLabel.TWIN_PERSISTED),
                        AcknowledgementRequest.of(REQUESTED_ACK))
                .timeout(COMMAND_TIMEOUT)
                .build())
        )
                .whenComplete((thingAsPersisted, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    assertThat(thingAsPersisted).isEqualTo(thing);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void createThingHandlingCustomAcknowledgementWithCustomPayload() throws Exception {
        clientSubscriber =
                newDittoClientV2(userSubscriber, Collections.singleton(DECLARED_ACK), Collections.emptyList());
        LOGGER.info("Created dittoClientSubscriber");
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final Thing thing = ThingFactory.newThing(thingId).setPolicyId(policyId);
        final Policy policy = newPolicy(policyId, user, userSubscriber);
        final JsonArray customAckPayload = JsonArray.newBuilder().add(1, 2, 3, 5, 8, 13, 21).build();

        clientSubscriber.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(),
                thingChange -> {
                    LOGGER.debug("received ThingChange {}", thingChange);

                    if (ChangeAction.CREATED.equals(thingChange.getAction())) {
                        assertThat(thingChange.getThing().flatMap(Thing::getEntityId)).hasValue(thingId);
                        assertThat(thingChange.getThing()).hasValue(thing);
                        assertThat(thingChange.getDittoHeaders().getAcknowledgementRequests()).contains(
                                AcknowledgementRequest.of(REQUESTED_ACK));

                        thingChange.handleAcknowledgementRequest(REQUESTED_ACK, ackRequest ->
                                ackRequest.acknowledge(HttpStatus.OK, customAckPayload));

                        latch.countDown();
                    }
                });

        startConsumptionAndWait(clientSubscriber.twin());

        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), DittoHeaders.newBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .acknowledgementRequest(
                        AcknowledgementRequest.of(DittoAcknowledgementLabel.TWIN_PERSISTED),
                        AcknowledgementRequest.of(REQUESTED_ACK))
                .timeout(COMMAND_TIMEOUT)
                .build());

        client.sendDittoProtocol(DITTO_PROTOCOL_ADAPTER.toAdaptable(createThing))
                .whenComplete((adaptableResponse, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Error in Test", throwable);
                    }

                    final Signal<?> responseSignal = DITTO_PROTOCOL_ADAPTER.fromAdaptable(adaptableResponse);
                    assertThat(responseSignal).isInstanceOf(Acknowledgements.class);

                    final Acknowledgements acknowledgements = (Acknowledgements) responseSignal;
                    assertThat(acknowledgements.getSuccessfulAcknowledgements()).hasSize(2);

                    assertThat(acknowledgements.getSuccessfulAcknowledgements()).anySatisfy(ack -> {
                        final SoftAssertions softly = new SoftAssertions();
                        softly.assertThat((CharSequence) ack.getLabel())
                                .isEqualTo(DittoAcknowledgementLabel.TWIN_PERSISTED);
                        softly.assertThat((CharSequence) ack.getEntityId()).isEqualTo(thingId);
                        softly.assertThat(ack.getHttpStatus()).isEqualTo(HttpStatus.CREATED);
                        softly.assertAll();
                    });

                    assertThat(acknowledgements.getSuccessfulAcknowledgements()).anySatisfy(ack -> {
                        final SoftAssertions softly = new SoftAssertions();
                        softly.assertThat((CharSequence) ack.getLabel()).isEqualTo(REQUESTED_ACK);
                        softly.assertThat((CharSequence) ack.getEntityId()).isEqualTo(thingId);
                        softly.assertThat(ack.getHttpStatus()).isEqualTo(HttpStatus.OK);
                        softly.assertThat(ack.getEntity()).contains(customAckPayload);
                        softly.assertAll();
                    });

                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void subscriberWithoutDeclaredAck() throws Exception {
        clientSubscriber = newDittoClientV2(userSubscriber);
        LOGGER.info("Created dittoClientSubscriber");
        final CountDownLatch latch = new CountDownLatch(1);

        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final Thing thing = ThingFactory.newThing(thingId);
        final Policy policy = newPolicy(PolicyId.of(thingId), user, userSubscriber);

        clientSubscriber.twin().forId(thingId).registerForThingChanges(UUID.randomUUID().toString(),
                thingChange -> {
                    LOGGER.debug("received ThingChange {}", thingChange);

                    if (ChangeAction.CREATED.equals(thingChange.getAction())) {
                        assertThat(thingChange.getThing().flatMap(Thing::getEntityId)).hasValue(thingId);
                        assertThat(thingChange.getThing()).hasValue(thing);
                        assertThat(thingChange.getDittoHeaders().getAcknowledgementRequests()).contains(
                                AcknowledgementRequest.of(REQUESTED_ACK));
                        assertThat(thingChange.getDittoHeaders().getAcknowledgementRequests()).isEmpty();

                        latch.countDown();
                    }
                });

        startConsumptionAndWait(clientSubscriber.twin());

        client.twin().create(thing, policy, Options.headers(DittoHeaders.newBuilder()
                .acknowledgementRequest(
                        AcknowledgementRequest.of(DittoAcknowledgementLabel.TWIN_PERSISTED),
                        AcknowledgementRequest.of(REQUESTED_ACK))
                .timeout(COMMAND_TIMEOUT)
                .build())
        )
                .handle((thingAsPersisted, throwable) -> {
                    final Throwable cause = throwable.getCause();
                    assertThat(cause).isInstanceOf(AcknowledgementsFailedException.class);
                    final AcknowledgementsFailedException acknowledgementsFailedException =
                            (AcknowledgementsFailedException) cause;
                    final Set<Acknowledgement> failedAcknowledgements =
                            acknowledgementsFailedException.getAcknowledgements().getFailedAcknowledgements();
                    assertThat(failedAcknowledgements).hasSize(1);
                    assertThat(failedAcknowledgements).allSatisfy(ack -> {
                        final SoftAssertions softly = new SoftAssertions();
                        softly.assertThat((CharSequence) ack.getLabel()).isEqualTo(REQUESTED_ACK);
                        softly.assertThat((CharSequence) ack.getEntityId()).isEqualTo(thingId);
                        softly.assertThat(ack.getHttpStatus()).isEqualTo(HttpStatus.REQUEST_TIMEOUT);
                        softly.assertAll();
                    });
                    latch.countDown();
                    return null;
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, SECONDS);

        awaitLatchTrue(latch);
    }

    @Test
    public void subscribeTwoTimesWithSameDeclaredAcks() throws Exception {

        final AcknowledgementLabel acknowledgementLabel =
                AcknowledgementLabel.of(serviceEnv.getTestingContext2().getSolution().getUsername() + ":" + name.getMethodName());
        final CompletableFuture<Void> future1 = new CompletableFuture<>();
        final CompletableFuture<Void> future2 = new CompletableFuture<>();
        final DittoClient dittoClient1 =
                newDittoClientV2(userSubscriber, List.of(acknowledgementLabel), List.of(),
                        future1::completeExceptionally);
        final DittoClient dittoClient2 =
                newDittoClientV2(userSubscriber, List.of(acknowledgementLabel), List.of(),
                        future2::completeExceptionally);

        future2.exceptionally(throwable -> {
            assertThat(throwable).isInstanceOf(AcknowledgementLabelNotUniqueException.class);
            return null;
        }).get(5L, SECONDS);

        assertThat(future1).isNotCompletedExceptionally();
        assertThat(future2).isCompletedExceptionally();

        // if an assertion failed, the clients are destroyed in @AfterClass.
        dittoClient1.destroy();
        dittoClient2.destroy();
    }

    @Test
    public void subscribeWithInvalidDeclaredAck() {
        final AcknowledgementLabel acknowledgementLabel = AcknowledgementLabel.of("wrong:test");
        final List<AcknowledgementLabel> wrongDeclaredAcknowledgements = List.of(acknowledgementLabel);
        final List<AcknowledgementLabel> emptyAcks = List.of();
        assertThatExceptionOfType(CompletionException.class)
                .isThrownBy(() -> newDittoClientV2(userSubscriber, wrongDeclaredAcknowledgements, emptyAcks))
                .withCauseInstanceOf(AuthenticationException.class)
                .withMessageContaining(
                        "{\"status\":400,\"error\":\"acknowledgement:label.invalid\",\"message\":\"Acknowledgement label <wrong:test> is invalid.\"");
    }

    @Test
    public void publishNotDeclaredAck() {
        final Adaptable notDeclaredAck = DittoProtocolAdapter.newInstance()
                .toAdaptable(Acknowledgement.of(AcknowledgementLabel.of("not-declared"), ThingId.generateRandom(),
                        HttpStatus.OK, DittoHeaders.empty()));
        client.sendDittoProtocol(notDeclaredAck)
                .thenAccept(response -> assertThat(response.getPayload().getValue())
                        .contains(JsonObject.of("{" +
                                "\"status\":400," +
                                "\"error\":\"acknowledgement:label.not.declared\"," +
                                "\"message\":\"Cannot send acknowledgement with label <not-declared>, " +
                                "which is not declared.\"," +
                                "\"description\":\"Each connection may only send acknowledgements whose label " +
                                "matches one declared for the connection.\"" +
                                "}")))
                .toCompletableFuture()
                .join();
    }

}
