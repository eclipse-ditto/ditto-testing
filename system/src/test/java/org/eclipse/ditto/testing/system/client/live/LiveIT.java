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
package org.eclipse.ditto.testing.system.client.live;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.ditto.base.model.common.HttpStatus.ACCEPTED;
import static org.eclipse.ditto.testing.common.TestConstants.API_V_2;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.acks.AcknowledgementRequest;
import org.eclipse.ditto.base.model.acks.DittoAcknowledgementLabel;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.changes.ChangeAction;
import org.eclipse.ditto.client.live.commands.LiveCommandHandler;
import org.eclipse.ditto.client.live.commands.modify.CreateThingLiveCommand;
import org.eclipse.ditto.client.live.commands.modify.CreateThingLiveCommandAnswerBuilder;
import org.eclipse.ditto.client.management.AcknowledgementsFailedException;
import org.eclipse.ditto.client.options.Options;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.messages.model.MessageDirection;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.http.ContentType;

/**
 * Integration test for {@link org.eclipse.ditto.client.live.Live} client. Tests the following scenarios:
 * <ul>
 * <li>A live client registers for handling live commands, another live client sends commands.</li>
 * <li>A live client registers for receiving live events, another live client emits events.</li>
 * </ul>
 */
public final class LiveIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(LiveIT.class);

    private AuthClient user;
    private AuthClient subscriber;
    private AuthClient blockedUser;

    private static final AcknowledgementLabel DECLARED_ACK =
            AcknowledgementLabel.of(serviceEnv.getTestingContext2().getSolution().getUsername() + ":handled-by-dittoClientSubscriber");
    private static final AcknowledgementLabel REQUESTED_ACK =
            AcknowledgementLabel.of(
                    serviceEnv.getTestingContext2().getSolution().getUsername() + ":handled-by-dittoClientSubscriber");
    private DittoClient dittoClient;
    private DittoClient dittoClientSubscriber;

    @Rule
    public TestName name = new TestName();

    @Before
    public void setUp() {
        LOGGER.info("-----> Running Test: {}", name.getMethodName());

        user = serviceEnv.getDefaultTestingContext().getOAuthClient();
        subscriber = serviceEnv.getTestingContext2().getOAuthClient();

        dittoClient = newDittoClientV2(user);
        dittoClientSubscriber = newDittoClientV2(subscriber, Collections.emptyList(), Collections.singleton(
                DECLARED_ACK));
    }

    @After
    public void tearDown() {
        shutdownClient(dittoClient);
        shutdownClient(dittoClientSubscriber);
    }

    @Test
    public void testHandlingLiveCommands() {
        final ThingId thingId = ThingId.of(idGenerator().withSuffixedRandomName("liveTestLiveCommands"));

        final CompletableFuture<Boolean> responseFuture = new CompletableFuture<>();

        dittoClientSubscriber.live().handleCreateThingCommands(command -> {
            LOGGER.info("live command subscriber got CreateThing command: {}", command);
            return command.answer()
                    .withResponse(CreateThingLiveCommandAnswerBuilder.ResponseFactory::created)
                    .withEvent(CreateThingLiveCommandAnswerBuilder.EventFactory::created);
        });
        dittoClientSubscriber.live().startConsumption().toCompletableFuture().join();

        // first create Twin, otherwise policy check will forbid sending a "live" create:
        final Thing newThing = ThingsModelFactory.newThingBuilder().setId(thingId).build();
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        LOGGER.info("Creating TWIN Thing: {}", thingId);
        putThingWithPolicy(2, newThing, policy,
                JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        LOGGER.info("Creating LIVE Thing: {}", newThing);
        dittoClient.live()
                .create(thingId)
                .whenComplete((thing, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Received error when creating the thing.", throwable);
                        responseFuture.completeExceptionally(throwable);
                    } else if (thing.getEntityId().filter(thingId::equals).isPresent()) {
                        responseFuture.complete(true);
                    } else {
                        LOGGER.warn("Received unexpected thing <{}>.", thing);
                        responseFuture.completeExceptionally(new AssertionError("Received unexpected thing " + thing));
                    }
                });

        responseFuture.completeOnTimeout(false, TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!responseFuture.join()) {
            throw new AssertionError("Did not receive a response.");
        }
    }

    @Test
    public void testHandlingLiveCommandsWithAcknowledgement() {
        final ThingId thingId = ThingId.of(idGenerator().withSuffixedRandomName("liveTestLiveCommands"));
        final CompletableFuture<Boolean> responseFuture = new CompletableFuture<>();

        dittoClientSubscriber.live().register(LiveCommandHandler.withAcks(CreateThingLiveCommand.class, liveCommand -> {
            LOGGER.info("live command subscriber got CreateThing command: {}", liveCommand.getLiveCommand());
            liveCommand.handleAcknowledgementRequest(REQUESTED_ACK,
                    handle -> handle.acknowledge(HttpStatus.IM_USED));
            return liveCommand.answer()
                    .withResponse(CreateThingLiveCommandAnswerBuilder.ResponseFactory::created)
                    .withoutEvent();
        }));
        dittoClientSubscriber.live().startConsumption().toCompletableFuture().join();

        // first create Twin, otherwise policy check will forbid sending a "live" create:
        final Thing newThing = ThingsModelFactory.newThingBuilder().setId(thingId).build();
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        LOGGER.info("Creating TWIN Thing: {}", thingId);
        putThingWithPolicy(2, newThing, policy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        LOGGER.info("Creating LIVE Thing: {}", newThing);
        dittoClient.live()
                .create(thingId, Options.headers(DittoHeaders.newBuilder()
                        .acknowledgementRequest(AcknowledgementRequest.of(DittoAcknowledgementLabel.LIVE_RESPONSE),
                                AcknowledgementRequest.of(REQUESTED_ACK))
                        .timeout(Duration.ofSeconds(2))
                        .build()))
                .whenComplete((thing, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("Received error when creating the thing.", throwable);
                        responseFuture.completeExceptionally(throwable);
                    } else if (thing.getEntityId().filter(thingId::equals).isPresent()) {
                        responseFuture.complete(true);
                    } else {
                        LOGGER.warn("Received unexpected thing <{}>.", thing);
                        responseFuture.completeExceptionally(new AssertionError("Received unexpected thing " + thing));
                    }
                });

        responseFuture.completeOnTimeout(false, TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!responseFuture.join()) {
            throw new AssertionError("Did not receive any response or acknowledgement.");
        }
    }

    @Test
    public void testHandlingLiveCommandsWithNegativeAcknowledgement() {
        final ThingId thingId = ThingId.of(idGenerator().withSuffixedRandomName("liveTestLiveCommands"));
        final CompletableFuture<Boolean> responseFuture = new CompletableFuture<>();

        dittoClientSubscriber.live().register(LiveCommandHandler.withAcks(CreateThingLiveCommand.class, liveCommand -> {
            LOGGER.info("live command subscriber got CreateThing command: {}", liveCommand.getLiveCommand());
            liveCommand.handleAcknowledgementRequest(REQUESTED_ACK,
                    handle -> handle.acknowledge(HttpStatus.IM_A_TEAPOT));
            return liveCommand.answer()
                    .withResponse(CreateThingLiveCommandAnswerBuilder.ResponseFactory::created)
                    .withoutEvent();
        }));
        dittoClientSubscriber.live().startConsumption().toCompletableFuture().join();

        // first create Twin, otherwise policy check will forbid sending a "live" create:
        final Thing newThing = ThingsModelFactory.newThingBuilder().setId(thingId).build();
        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        LOGGER.info("Creating TWIN Thing: {}", thingId);
        putThingWithPolicy(2, newThing, policy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        LOGGER.info("Creating LIVE Thing: {}", newThing);
        dittoClient.live()
                .create(thingId, Options.headers(DittoHeaders.newBuilder()
                        .acknowledgementRequest(AcknowledgementRequest.of(DittoAcknowledgementLabel.LIVE_RESPONSE),
                                AcknowledgementRequest.of(REQUESTED_ACK))
                        .build()))
                .whenComplete((thing, throwable) -> {
                    try {
                        assertThat(thing).describedAs("Expect live command to fail, but it did not").isNull();
                        assertThat(throwable).isInstanceOf(CompletionException.class)
                                .hasCauseInstanceOf(AcknowledgementsFailedException.class);
                        final AcknowledgementsFailedException exception =
                                (AcknowledgementsFailedException) throwable.getCause();
                        assertThat(exception.getAcknowledgements()
                                .getAcknowledgement(DittoAcknowledgementLabel.LIVE_RESPONSE)
                                .orElseThrow()
                                .getHttpStatus())
                                .isEqualTo(HttpStatus.CREATED);
                        assertThat(exception.getAcknowledgements()
                                .getAcknowledgement(REQUESTED_ACK)
                                .orElseThrow()
                                .getHttpStatus())
                                .isEqualTo(HttpStatus.IM_A_TEAPOT);
                        responseFuture.complete(true);
                    } catch (final Throwable e) {
                        responseFuture.completeExceptionally(e);
                    }
                });

        responseFuture.completeOnTimeout(false, TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (!responseFuture.join()) {
            throw new AssertionError("Did not receive any response or acknowledgement.");
        }
    }

    @Test
    public void testReceivingLiveChanges() throws Exception {
        final ThingId thingId = ThingId.of(idGenerator().withSuffixedRandomName("liveTestLiveChanges"));

        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(PolicyId.of(thingId))
                .build();

        final CountDownLatch eventLatch = new CountDownLatch(1);

        dittoClientSubscriber.live().registerForThingChanges("", change -> {
            if (change.getAction().equals(ChangeAction.CREATED) && change.getEntityId().equals(thingId)) {
                eventLatch.countDown();
            } else {
                LOGGER.warn("Received unexpected change {}.", change);
            }
        });

        dittoClientSubscriber.live().startConsumption().toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        dittoClient.twin().create(thing, policy)
                .whenComplete((createdThing, error) -> {
                    assertThat(error).isNull();
                    assertThat(createdThing).isEqualTo(thing);
                })
                .toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        dittoClient.live().emitEvent(factory -> factory.thingCreated(thing));

        awaitLatchTrue(eventLatch);
    }


    @Test
    public void testMessageConsumptionWithMultipleHandlers()
            throws ExecutionException, InterruptedException, TimeoutException {
        final ThingId thingId =
                ThingId.of(idGenerator().withSuffixedRandomName("messageConsumptionWithMultipleHandlers"));
        final String featureId = "Uploadable";
        final String messageSubject = "messageSubject";

        final Policy policy = newPolicy(PolicyId.of(thingId), user, subscriber);
        final Thing thing = ThingsModelFactory.newThingBuilder()
                .setId(thingId)
                .setPolicyId(PolicyId.of(thingId))
                .build();

        final CountDownLatch latch = new CountDownLatch(3);

        dittoClientSubscriber.live().registerForMessage("ALL_THINGS", messageSubject, JsonValue.class,
                message -> {
                    assertThat(message.getEntityId().getName()).isEqualTo(thingId.getName());
                    assertThat(message.getFeatureId()).contains(featureId);
                    assertThat(message.getPayload()).contains(JsonValue.of(42));
                    latch.countDown();
                });

        dittoClientSubscriber.live().forId(thingId).registerForMessage("SINGLE_THING", messageSubject,
                JsonValue.class, message -> {
                    assertThat(message.getEntityId().getName()).isEqualTo(thingId.getName());
                    assertThat(message.getFeatureId()).contains(featureId);
                    assertThat(message.getPayload()).contains(JsonValue.of(42));
                    latch.countDown();
                });

        dittoClientSubscriber.live().forFeature(thingId, featureId).registerForMessage("SINGLE_FEATURE",
                messageSubject, JsonValue.class, message -> {
                    assertThat(message.getEntityId().getName()).isEqualTo(thingId.getName());
                    assertThat(message.getFeatureId()).contains(featureId);
                    assertThat(message.getPayload()).contains(JsonValue.of(42));
                    latch.countDown();
                });

        dittoClientSubscriber.live().startConsumption().toCompletableFuture()
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        putThingWithPolicy(2, thing, policy, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        // send feature message via HTTP API
        postMessage(API_V_2, thingId, featureId, MessageDirection.TO, messageSubject, ContentType.JSON,
                JsonFactory.newValue(42).toString(), "0")
                .expectingHttpStatus(ACCEPTED)
                .fire();

        awaitLatchTrue(latch);
    }

}
