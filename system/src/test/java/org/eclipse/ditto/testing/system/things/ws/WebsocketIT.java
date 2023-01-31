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
package org.eclipse.ditto.testing.system.things.ws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;
import static org.eclipse.ditto.testing.common.TestConstants.Policy.ARBITRARY_SUBJECT_TYPE;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.acks.AcknowledgementRequest;
import org.eclipse.ditto.base.model.acks.DittoAcknowledgementLabel;
import org.eclipse.ditto.base.model.common.DittoDuration;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.exceptions.DittoRuntimeException;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgement;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgements;
import org.eclipse.ditto.base.model.signals.commands.CommandResponse;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.messages.model.Message;
import org.eclipse.ditto.messages.model.MessageDirection;
import org.eclipse.ditto.messages.model.MessageHeaders;
import org.eclipse.ditto.messages.model.MessagesModelFactory;
import org.eclipse.ditto.messages.model.signals.commands.SendThingMessage;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectAnnouncement;
import org.eclipse.ditto.policies.model.SubjectExpiry;
import org.eclipse.ditto.policies.model.SubjectId;
import org.eclipse.ditto.policies.model.SubjectType;
import org.eclipse.ditto.policies.model.signals.announcements.SubjectDeletionAnnouncement;
import org.eclipse.ditto.policies.model.signals.commands.modify.CreatePolicy;
import org.eclipse.ditto.policies.model.signals.commands.modify.CreatePolicyResponse;
import org.eclipse.ditto.policies.model.signals.commands.modify.DeletePolicy;
import org.eclipse.ditto.policies.model.signals.commands.modify.DeletePolicyResponse;
import org.eclipse.ditto.policies.model.signals.commands.query.RetrievePolicy;
import org.eclipse.ditto.policies.model.signals.commands.query.RetrievePolicyResponse;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.TopicPath;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
import org.eclipse.ditto.protocol.adapter.ProtocolAdapter;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ServiceEnvironment;
import org.eclipse.ditto.testing.common.Solution;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.ThingsSubjectIssuer;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.common.ws.ThingsWebsocketClient;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.ThingErrorResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThingResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.DeleteThing;
import org.eclipse.ditto.things.model.signals.commands.modify.DeleteThingResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.MergeThing;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttribute;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyFeature;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyFeatureDesiredPropertyResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyFeatureProperty;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyFeatureResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThingResponse;
import org.eclipse.ditto.things.model.signals.events.AttributeModified;
import org.eclipse.ditto.things.model.signals.events.FeatureDesiredPropertyModified;
import org.eclipse.ditto.things.model.signals.events.FeaturePropertyModified;
import org.eclipse.ditto.things.model.signals.events.ThingCreated;
import org.eclipse.ditto.things.model.signals.events.ThingMerged;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for basic web socket functionality.
 */
public final class WebsocketIT extends IntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebsocketIT.class);
    private static final long LATCH_TIMEOUT_SECONDS = 30;
    private static final AtomicInteger ACK_COUNTER = new AtomicInteger(0);

    // not static: refresh correlation ID for each test
    private final DittoHeaders COMMAND_HEADERS_V2 = DittoHeaders.newBuilder()
            .schemaVersion(JsonSchemaVersion.V_2)
            .randomCorrelationId()
            .build();

    private String declaredAckClient1;
    private String declaredAckClient2;
    private String requestedAckClient2;
    private ThingsWebsocketClient clientUser1;
    private ThingsWebsocketClient clientUser2;
    private ThingsWebsocketClient clientUser3;
    private ThingsWebsocketClient clientUser4;
    private ThingsWebsocketClient clientUser5;
    private ThingsWebsocketClient clientUserWithBlockedSolution;
    private ThingsWebsocketClient clientUser2ViaQueryParam;

    private TestingContext testingContext1;
    private AuthClient user1;
    private TestingContext testingContext2;
    private AuthClient user2;
    private ThingsWebsocketClient secondClientForDefaultSolution;

    @Before
    public void setUpClients() {
        final Solution solution1 = ServiceEnvironment.createSolutionWithRandomUsernameRandomNamespace();
        testingContext1 = TestingContext.withGeneratedMockClient(solution1, TEST_CONFIG);
        user1 = testingContext1.getOAuthClient();
        final Solution solution2 = ServiceEnvironment.createSolutionWithRandomUsernameRandomNamespace();
        testingContext2 = TestingContext.withGeneratedMockClient(solution2, TEST_CONFIG);
        user2 = testingContext2.getOAuthClient();

        declaredAckClient1 =
                MessageFormat.format("{0}:custom1-" + ACK_COUNTER.incrementAndGet(), testingContext1.getSolution().getUsername());
        final String custom2 = "custom2-" + ACK_COUNTER.incrementAndGet();
        declaredAckClient2 =
                MessageFormat.format("{0}:" + custom2, testingContext2.getSolution().getUsername());
        requestedAckClient2 =
                MessageFormat.format("{0}:" + custom2, testingContext2.getSolution().getUsername());
        clientUser1 = newTestWebsocketClient(user1.getAccessToken(),
                Map.of(DittoHeaderDefinition.DECLARED_ACKS.getKey(), "[\"" + declaredAckClient1 + "\"]"),
                TestConstants.API_V_2);
        clientUser2 = newTestWebsocketClient(user2.getAccessToken(),
                Map.of(DittoHeaderDefinition.DECLARED_ACKS.getKey(), "[\"" + declaredAckClient2 + "\"]"),
                TestConstants.API_V_2);
        clientUser2ViaQueryParam = newTestWebsocketClient(user2.getAccessToken(), Map.of(),
                TestConstants.API_V_2, ThingsWebsocketClient.JwtAuthMethod.QUERY_PARAM);

        clientUser1.connect("ThingsWebsocketClient-User1-" + UUID.randomUUID());
        clientUser2.connect("ThingsWebsocketClient-User2-" + UUID.randomUUID());
        clientUser2ViaQueryParam.connect("ThingsWebsocketClient-User2-QueryParam-" + UUID.randomUUID());

        final AuthClient user3 = serviceEnv.getTestingContext3().getOAuthClient();
        final AuthClient user4 = serviceEnv.getTestingContext4().getOAuthClient();
        final AuthClient user5 = serviceEnv.getTestingContext5().getOAuthClient();
        clientUser3 = newTestWebsocketClient(user3.getAccessToken(), Map.of(), TestConstants.API_V_2);
        clientUser4 = newTestWebsocketClient(user4.getAccessToken(), Map.of(), TestConstants.API_V_2);
        clientUser5 = newTestWebsocketClient(user5.getAccessToken(), Map.of(), TestConstants.API_V_2);
        clientUserWithBlockedSolution =
                newTestWebsocketClient(user2.getAccessToken(), Map.of(), TestConstants.API_V_2);

        clientUser3.connect("ThingsWebsocketClient-User3-" + UUID.randomUUID());
        clientUser4.connect("ThingsWebsocketClient-User4-" + UUID.randomUUID());
        clientUser5.connect("ThingsWebsocketClient-User5-" + UUID.randomUUID());
        clientUserWithBlockedSolution.connect("ThingsWebsocketClient-User4-" + UUID.randomUUID());

        secondClientForDefaultSolution = newTestWebsocketClient(this.user1.getAccessToken(), Map.of(),
                TestConstants.API_V_2);
        secondClientForDefaultSolution.connect("ThingsWebsocketClient-User5-" + UUID.randomUUID());
    }

    @After
    public void tearDownClients() {
        if (clientUser1 != null) {
            clientUser1.disconnect();
        }
        if (clientUser2 != null) {
            clientUser2.disconnect();
        }
        if (clientUser3 != null) {
            clientUser3.disconnect();
        }
        if (clientUser4 != null) {
            clientUser4.disconnect();
        }
        if (clientUser5 != null) {
            clientUser5.disconnect();
        }
        if (clientUserWithBlockedSolution != null) {
            clientUserWithBlockedSolution.disconnect();
        }
    }

    @Test
    public void createThingWithCopiedPolicy() throws Exception {
        final Policy policyToCopy = prepareSpecialPolicyToCopy();
        final String originalPolicyId = policyToCopy.getEntityId()
                .map(String::valueOf)
                .orElseThrow(() -> new IllegalStateException("There must be a Policy ID."));
        final Thing originalThing = prepareThingToHoldPolicyToCopy();

        final CreateThing createThing = CreateThing.of(originalThing, policyToCopy.toJson(), COMMAND_HEADERS_V2);

        // Create original Thing
        clientUser1.send(createThing).whenComplete((commandResponse, throwable) -> {
            assertThat(throwable).isNull();
            assertThat(commandResponse).isInstanceOf(CreateThingResponse.class);
            final ThingId thingIdWithCopiedPolicy =
                    ThingId.of(idGenerator(testingContext1.getSolution().getDefaultNamespace())
                            .withPrefixedRandomName("thingWithCopiedPolicy"));
            final Thing thingWithCopiedPolicy = Thing.newBuilder().setId(thingIdWithCopiedPolicy).build();
            final CreateThing createThingWithCopiedPolicy =
                    CreateThing.withCopiedPolicy(thingWithCopiedPolicy, originalPolicyId, COMMAND_HEADERS_V2);
            // Create Thing with copied policy
            clientUser1.send(createThingWithCopiedPolicy).whenComplete((commandResponse2, throwable2) ->
                    assertCopiedPolicy(commandResponse2, throwable2, policyToCopy));
        }).get(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void modifyThingWithCopiedPolicy() throws Exception {

        final Policy policyToCopy = prepareSpecialPolicyToCopy();
        final String originalPolicyId = policyToCopy.getEntityId()
                .map(String::valueOf)
                .orElseThrow(() -> new IllegalStateException("There must be a Policy ID."));
        final Thing originalThing = prepareThingToHoldPolicyToCopy();
        final ThingId originalThingId = originalThing.getEntityId()
                .orElseThrow(() -> new IllegalStateException("There must be an ID"));

        final ModifyThing modifyThing =
                ModifyThing.of(originalThingId, originalThing, policyToCopy.toJson(), COMMAND_HEADERS_V2);

        // Create original Thing
        clientUser1.send(modifyThing).whenComplete((commandResponse, throwable) -> {
            assertThat(throwable).isNull();
            assertThat(commandResponse).isInstanceOf(CreateThingResponse.class);
            final ThingId thingIdOfCopied = ThingId.of(
                    idGenerator(testingContext1.getSolution().getDefaultNamespace()).withPrefixedRandomName("thingWithCopiedPolicy"));
            final Thing thingWithCopiedPolicy = Thing.newBuilder().setId(thingIdOfCopied).build();
            final ModifyThing modifyThingWithCopiedPolicy =
                    ModifyThing.withCopiedPolicy(thingIdOfCopied, thingWithCopiedPolicy, originalPolicyId,
                            COMMAND_HEADERS_V2);
            clientUser1.send(modifyThingWithCopiedPolicy).whenComplete((commandResponse2, throwable2) ->
                    assertCopiedPolicy(commandResponse2, throwable2, policyToCopy));
        }).get(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }


    private Thing prepareThingToHoldPolicyToCopy() {
        final ThingId originalThingId = ThingId.of(
                idGenerator(testingContext1.getSolution().getDefaultNamespace()).withPrefixedRandomName("thingWithOriginalPolicy"));
        return Thing.newBuilder().setId(originalThingId).build();
    }

    private Policy prepareSpecialPolicyToCopy() {
        final PolicyId originalPolicyId = PolicyId.of(
                idGenerator(testingContext1.getSolution().getDefaultNamespace()).withPrefixedRandomName("policyToCopy"));
        final String clientId = testingContext1.getOAuthClient().getClientId();
        return PoliciesModelFactory.newPolicyBuilder(originalPolicyId)
                .forLabel("specialCopiedLabel")
                .setSubject(ThingsSubjectIssuer.DITTO, clientId, ARBITRARY_SUBJECT_TYPE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), WRITE, READ)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), WRITE, READ)
                .build();
    }

    private void assertCopiedPolicy(final CommandResponse commandResponse, final Throwable throwable,
            final Policy policyToCopy) {
        assertThat(throwable).isNull();
        assertThat(commandResponse).isInstanceOf(CreateThingResponse.class);
        final PolicyId policyIdOfCopiedPolicy = ((CreateThingResponse) commandResponse).getThingCreated()
                .orElseThrow(() -> new IllegalStateException("There must be a Thing"))
                .getPolicyId()
                .orElseThrow(() -> new IllegalStateException("There must be a policyId"));
        final RetrievePolicy retrievePolicy = RetrievePolicy.of(policyIdOfCopiedPolicy, COMMAND_HEADERS_V2);

        // Assert Policy is copied
        clientUser1.send(retrievePolicy).whenComplete((policyResponse, policyError) -> {
            assertThat(policyError).isNull();
            assertThat(policyResponse).isInstanceOf(RetrievePolicyResponse.class);
            final Policy actualPolicy = ((RetrievePolicyResponse) policyResponse).getPolicy();
            assertThat(actualPolicy.getEntriesSet()).isEqualTo(policyToCopy.getEntriesSet());
            assertThat(actualPolicy.getLabels()).isEqualTo(policyToCopy.getLabels());
        });
    }

    @Test
    public void createThing() throws Exception {
        final Thing thing = Thing.newBuilder()
                .setId(ThingId.of(idGenerator(testingContext1.getSolution().getDefaultNamespace()).withRandomName()))
                .build();

        final CreateThing createThing = CreateThing.of(thing, null, COMMAND_HEADERS_V2);

        clientUser1.send(createThing).whenComplete((commandResponse, throwable) -> {
            assertThat(throwable).isNull();
            assertThat(commandResponse).isInstanceOf(CreateThingResponse.class);
        }).get(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    @Category(Acceptance.class)
    public void retrieveThing() throws InterruptedException, TimeoutException, ExecutionException {
        final ThingId thingId = ThingId.of(idGenerator(testingContext1.getSolution().getDefaultNamespace()).withRandomName());

        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();

        final Policy policy = newPolicy(PolicyId.of(thingId), List.of(user1, user2), List.of(user1));
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), commandHeadersWithOwnCorrelationId());

        clientUser1.send(createThing).whenComplete((commandResponse, throwable) -> {
            assertThat(throwable).isNull();
            assertThat(commandResponse).isInstanceOf(CreateThingResponse.class);
        }).get(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        final RetrieveThing retrieveThing = RetrieveThing.of(thingId, commandHeadersWithOwnCorrelationId());

        clientUser2.send(retrieveThing).whenComplete((commandResponse, throwable) -> {
            assertThat(throwable).isNull();
            assertThat(commandResponse).isInstanceOf(RetrieveThingResponse.class);
        }).get(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        clientUser2ViaQueryParam.send(retrieveThing).whenComplete((commandResponse, throwable) -> {
            assertThat(throwable).isNull();
            assertThat(commandResponse).isInstanceOf(RetrieveThingResponse.class);
        }).get(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    @Category(Acceptance.class)
    public void consumeThingCreated() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        clientUser2.startConsumingEvents(event -> {
            assertThat(event).isInstanceOf(ThingCreated.class);
            latch.countDown();
        }).join();

        final ThingId thingId = ThingId.of(idGenerator(testingContext1.getSolution().getDefaultNamespace()).withRandomName());
        final Policy policy = newPolicy(PolicyId.of(thingId), List.of(user1, user2), List.of(user1));
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();

        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), COMMAND_HEADERS_V2);

        clientUser1.send(createThing).whenComplete((commandResponse, throwable) -> {
            assertThat(throwable).isNull();
            assertThat(commandResponse).isInstanceOf(CreateThingResponse.class);
        });

        assertThat(latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void consumeFeatureModifiedHavingRevokedAndGrantedAuthIds() throws InterruptedException {

        // WHEN: a Thing with Policy is defining:
        //  - that user1 may read/write all of that Thing
        //  - that the group1 ("All users group") user1 belongs to may not read the "secret" feature
        final ThingId thingId = ThingId.of(idGenerator(testingContext1.getSolution().getDefaultNamespace()).withRandomName());

        final String clientId1 = user1.getClientId();
        final String clientId2 = user2.getClientId();

        final PolicyId policyId = PolicyId.of(thingId);
        final Policy policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("granted")
                .setSubject(ThingsSubjectIssuer.DITTO, clientId1,
                        SubjectType.newInstance("user1isBoss"))
                .setSubject(ThingsSubjectIssuer.DITTO, clientId2,
                        SubjectType.newInstance("user2isBoss"))
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), WRITE, READ)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), WRITE, READ)
                .forLabel("revoked")
                .setSubject(ThingsSubjectIssuer.DITTO, clientId1,
                        SubjectType.newInstance("group1mayNotReadSecretFeature"))
                .setRevokedPermissions(PoliciesResourceType.thingResource("/features/secret"), READ)
                .build();

        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setFeature("secret", FeatureProperties.newBuilder()
                        .set("wow", "that's a secret")
                        .build())
                .build();

        // WHEN: thing is created with Policy by user2
        final CountDownLatch creationLatch = new CountDownLatch(1);
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), COMMAND_HEADERS_V2);
        secondClientForDefaultSolution.send(createThing).whenComplete((commandResponse, throwable) -> {
            assertThat(throwable).isNull();
            assertThat(commandResponse).isInstanceOf(CreateThingResponse.class);
            creationLatch.countDown();
        });
        assertThat(creationLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

        // WHEN: user1 (also being in group1) subscribes for feature modified events
        final CountDownLatch negativeLatch = new CountDownLatch(1);
        clientUser1.startConsumingEvents(event -> {
            // THEN: this should not be called as the user1 has through his group1 a "revoke" to READ feature "secret"
            negativeLatch.countDown();
        }).join();

        // WHEN: feature "secret" gets modified by user2
        final ModifyFeature modifyFeature = ModifyFeature.of(thingId, Feature.newBuilder()
                .properties(FeatureProperties.newBuilder()
                        .set("wow", "that now changed")
                        .build()
                )
                .withId("secret")
                .build(), COMMAND_HEADERS_V2);
        secondClientForDefaultSolution.send(modifyFeature).whenComplete((commandResponse, throwable) -> {
            assertThat(throwable).isNull();
            assertThat(commandResponse).isInstanceOf(ModifyFeatureResponse.class);
        });

        // THEN: ensure the callback of clientUser1 was not invoked
        assertThat(negativeLatch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isFalse();
    }

    @Test
    @RunIf(DockerEnvironment.class)
    public void createGetAndDeleteThingForBlockedSolution()
            throws InterruptedException, TimeoutException, ExecutionException {
        final ThingId thingId = ThingId.of(idGenerator(testingContext1.getSolution().getDefaultNamespace()).withRandomName());
        final Policy policy = newPolicy(PolicyId.of(thingId), user1, user2);
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();

        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), COMMAND_HEADERS_V2);

        clientUser1.send(createThing).whenComplete((commandResponse, throwable) -> {
            LOGGER.info("Thing created - {}", commandResponse);
            assertThat(throwable).isNull();
            assertThat(commandResponse).isInstanceOf(CreateThingResponse.class);
        }).get(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);


        final RetrieveThing retrieveThing = RetrieveThing.of(thingId, COMMAND_HEADERS_V2);

        clientUserWithBlockedSolution.send(retrieveThing).whenComplete((commandResponse, throwable) -> {
            LOGGER.info("Thing retrieved - {}", commandResponse);
            assertThat(commandResponse).isInstanceOf(RetrieveThingResponse.class);
            assertThat(throwable).isNull();
        }).get(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        final DeleteThing deleteThing = DeleteThing.of(thingId, COMMAND_HEADERS_V2);

        clientUserWithBlockedSolution.send(deleteThing).whenComplete((commandResponse, throwable) -> {
            LOGGER.info("Thing deleted - {}", commandResponse);
            assertThat(commandResponse).isInstanceOf(DeleteThingResponse.class);
            assertThat(throwable).isNull();
        }).get(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void createThingAndModifyWithoutWaitingEnsuringEventOrder() throws Exception {
        final ThingId thingId = ThingId.of(idGenerator(testingContext1.getSolution().getDefaultNamespace()).withRandomName());

        final JsonPointer counterPointer = JsonPointer.of("counter");
        final Policy policy = newPolicy(PolicyId.of(thingId), user1, user2);
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setAttribute(counterPointer, JsonValue.of(0))
                .build();

        final int amountOfUpdates = 25;
        final CountDownLatch latch = new CountDownLatch(amountOfUpdates);
        final AtomicInteger counter = new AtomicInteger(0);

        clientUser2.startConsumingEvents(event -> {
            if (event instanceof final AttributeModified attributeModified) {
                if (counterPointer.equals(attributeModified.getAttributePointer())) {
                    final int updatedCounterValue = attributeModified.getAttributeValue().asInt();
                    final int expectedRevision = (int) attributeModified.getRevision() - 1;
                    final int expectedCounterValue = counter.incrementAndGet();

                    assertThat(updatedCounterValue).isEqualTo(expectedRevision);
                    assertThat(updatedCounterValue).isEqualTo(expectedCounterValue);
                    latch.countDown();
                }
            }
        }).join();

        final DittoHeaders noResponseHeaders = COMMAND_HEADERS_V2.toBuilder().responseRequired(false).build();
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), noResponseHeaders);
        clientUser1.send(createThing); // fire&forget

        for (int i = 1; i <= amountOfUpdates; i++) {
            final ModifyAttribute modifyAttr = ModifyAttribute.of(thingId, counterPointer, JsonValue.of(i),
                    noResponseHeaders.toBuilder()
                            .correlationId(COMMAND_HEADERS_V2.getCorrelationId().get() + "_" + i)
                            .build());
            clientUser1.send(modifyAttr); // fire&forget
        }

        assertThat(latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void createThingWithMultipleSlashesInFeatureProperty() {

        final ThingId thingId = ThingId.of(idGenerator(testingContext1.getSolution().getDefaultNamespace()).withRandomName());
        final TopicPath topicPath = TopicPath.newBuilder(thingId).things().twin().commands().modify().build();
        final String correlationId = UUID.randomUUID().toString();
        final String createFeatureWithMultipleSlashesInPath = "{" +
                "\"topic\": \"" + topicPath.getPath() + "\", " +
                "\"path\": \"/features/feature/properties/slash//es\", " +
                "\"headers\":{\"content-type\":\"application/vnd.eclipse.ditto+json\",\"version\":1,\"correlation-id\":\"" +
                correlationId + "\"}," +
                "\"payload\": 13" +
                "}";

        final long expectNoResponseSeconds = 5;
        assertThatExceptionOfType(TimeoutException.class).isThrownBy(() -> {
            try {
                clientUser1.sendWithResponse(createFeatureWithMultipleSlashesInPath, correlationId)
                        .whenComplete((commandResponse, throwable) -> Assert.fail(
                                "Should never get here as it is not possible to correlate the exception to the original message." +
                                        " The backend can't deserialize the message and thus does not know the correlationId."))
                        .get(expectNoResponseSeconds, TimeUnit.SECONDS);
            } catch (final Throwable e) {
                if (e instanceof CompletionException) {
                    throw e.getCause();
                } else {
                    throw e;
                }
            }
        });
    }

    @Test
    public void createThingAndChangeDesiredProperties() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);

        //Before
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(serviceEnv.getTestingContext4().getOAuthClient().getDefaultSubject())
                .setSubject(serviceEnv.getTestingContext5().getOAuthClient().getDefaultSubject())
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();

        final ThingId thingId = ThingId.of(
                serviceEnv.getTestingContext4().getSolution().getDefaultNamespace() +
                        ":" + UUID.randomUUID());

        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setFeatureDesiredProperties("testFeature", FeatureProperties.newBuilder().set("foo", "bar").build())
                .build();

        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), COMMAND_HEADERS_V2);

        clientUser4.send(createThing).whenComplete((commandResponse, throwable) -> {
            assertThat(throwable).isNull();
            assertThat(commandResponse).isInstanceOf(CreateThingResponse.class);
        });

        //Then
        clientUser5.startConsumingEvents(event -> {
            assertThat(event).isInstanceOf(FeatureDesiredPropertyModified.class);
            latch.countDown();
        }).join();

        final TopicPath topicPath = TopicPath.newBuilder(thingId).things().twin().commands().modify().build();

        final String modifyDesiredPropertyCommand = "{" +
                "\"topic\": \"" + topicPath.getPath() + "\", " +
                "\"path\": \"/features/testFeature/desiredProperties/foo\", " +
                "\"headers\":{\"content-type\":\"application/vnd.eclipse.ditto+json\",\"version\":2}," +
                "\"value\": \"baz\"" +
                "}";

        clientUser4.sendWithResponse(modifyDesiredPropertyCommand, "desiredPropertyTestCor")
                .whenComplete((response, throwable) -> {
                    assertThat(throwable).isNull();
                    assertThat(response).isInstanceOf(ModifyFeatureDesiredPropertyResponse.class);
                });

        assertThat(latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    public void subscribeForSignalsWithExtraFields() throws Exception {
        final ThingId thingId = ThingId.of(idGenerator(testingContext1.getSolution().getDefaultNamespace()).withRandomName());
        final Policy policy = newPolicy(PolicyId.of(thingId), user1, user2);
        final JsonPointer counterPointer = JsonPointer.of("counter");
        final JsonValue counterValue = JsonValue.of(5725);
        final JsonValue counterValueIncrementedByOne = JsonValue.of(5726);
        final JsonValue counterValueIncrementedByTwo = JsonValue.of(5727);
        final JsonObject expectedCounterAttribute = JsonObject.newBuilder()
                .set(JsonPointer.of("attributes").append(counterPointer), counterValue)
                .build();
        final JsonObject expectedCounterAttributeIncrByOne = JsonObject.newBuilder()
                .set(JsonPointer.of("attributes").append(counterPointer), counterValueIncrementedByOne)
                .build();
        final JsonObject expectedCounterAttributeIncrByTwo = JsonObject.newBuilder()
                .set(JsonPointer.of("attributes").append(counterPointer), counterValueIncrementedByTwo)
                .build();

        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setAttribute(counterPointer, counterValue)
                .build();

        final BlockingQueue<Adaptable> incomingMessages = new LinkedBlockingQueue<>();

        // clientUser2: push all incoming messages in a queue
        clientUser2.onAdaptable(incomingMessages::add);

        // GIVEN: clientUser2 subscribes to all signals with extra fields
        CompletableFuture.allOf(
                clientUser2.sendProtocolCommand("START-SEND-EVENTS?extraFields=attributes/counter",
                        "START-SEND-EVENTS:ACK"),
                clientUser2.sendProtocolCommand("START-SEND-MESSAGES?extraFields=attributes/counter",
                        "START-SEND-MESSAGES:ACK"),
                clientUser2.sendProtocolCommand("START-SEND-LIVE-EVENTS?extraFields=attributes/counter",
                        "START-SEND-LIVE-EVENTS:ACK"),
                clientUser2.sendProtocolCommand("START-SEND-LIVE-COMMANDS?extraFields=attributes/counter",
                        "START-SEND-LIVE-COMMANDS:ACK")
        ).join();

        // WHEN: clientUser1 sends twin command
        // THEN: clientUser2 receives enriched twin event
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), COMMAND_HEADERS_V2);
        clientUser1.send(createThing);
        final ProtocolAdapter protocolAdapter = DittoProtocolAdapter.newInstance();
        final Adaptable thingCreatedAdaptable = incomingMessages.take();
        assertThat(thingCreatedAdaptable.getPayload().getExtra()).contains(expectedCounterAttribute);
        assertThat(protocolAdapter.fromAdaptable(thingCreatedAdaptable))
                .isInstanceOf(ThingCreated.class)
                .extracting(signal -> ((ThingCreated) signal).getEntityId())
                .isEqualTo(thingId);

        // WHEN: clientUser1 sends twin command modifying the enriched data to a more recent value
        // THEN: clientUser2 receives enriched twin event with the more recent value
        final ModifyAttribute modifyAttribute = ModifyAttribute.of(thingId, counterPointer,
                JsonValue.of(counterValueIncrementedByOne), COMMAND_HEADERS_V2.toBuilder()
                        .randomCorrelationId()
                        .build());
        clientUser1.send(modifyAttribute);
        final Adaptable attributeModifiedAdaptable = incomingMessages.take();
        assertThat(attributeModifiedAdaptable.getPayload().getExtra()).contains(expectedCounterAttributeIncrByOne);
        assertThat(protocolAdapter.fromAdaptable(attributeModifiedAdaptable))
                .isInstanceOf(AttributeModified.class)
                .extracting(signal -> ((AttributeModified) signal).getEntityId())
                .isEqualTo(thingId);

        // WHEN: clientUser1 sends twin merge command modifying the enriched data to a more recent value
        // THEN: clientUser2 receives enriched ThingMerge twin event with the more recent value
        final MergeThing mergeThing = MergeThing.of(thingId, JsonPointer.of("/attributes").append(counterPointer),
                JsonValue.of(counterValueIncrementedByTwo), COMMAND_HEADERS_V2.toBuilder()
                        .randomCorrelationId()
                        .build());
        clientUser1.send(mergeThing);
        final Adaptable thingMergedAdaptable = incomingMessages.take();
        assertThat(thingMergedAdaptable.getPayload().getExtra()).contains(expectedCounterAttributeIncrByTwo);
        assertThat(protocolAdapter.fromAdaptable(thingMergedAdaptable))
                .isInstanceOf(ThingMerged.class)
                .extracting(signal -> ((ThingMerged) signal).getEntityId())
                .isEqualTo(thingId);

        // WHEN: clientUser1 sends live message
        // THEN: clientUser2 receives enriched live message
        final MessageHeaders messageHeaders =
                MessagesModelFactory.newHeadersBuilder(MessageDirection.TO, thingId, "JustSayingHello")
                        .contentType("text/plain")
                        .build();
        final String messagePayload = "Hello there, " + thingId;
        final Message<?> message = MessagesModelFactory.newMessageBuilder(messageHeaders)
                .payload(messagePayload)
                .build();
        clientUser1.send(SendThingMessage.of(thingId, message, COMMAND_HEADERS_V2.toBuilder()
                .randomCorrelationId()
                .build()));
        final Adaptable thingMessageAdaptable = incomingMessages.take();
        assertThat(thingMessageAdaptable.getPayload().getExtra()).contains(expectedCounterAttributeIncrByTwo);
        assertThat(protocolAdapter.fromAdaptable(thingMessageAdaptable))
                .isInstanceOf(SendThingMessage.class)
                .extracting(msg -> ((SendThingMessage<?>) msg).getMessage().getPayload())
                .isEqualTo(Optional.of(messagePayload));

        // WHEN: clientUser1 sends live command
        // THEN: clientUser2 receives enriched live command
        final DittoHeaders liveDittoHeaders = COMMAND_HEADERS_V2.toBuilder()
                .randomCorrelationId()
                .channel(TopicPath.Channel.LIVE.getName()).build();
        final CreateThing liveCommand = createThing.setDittoHeaders(liveDittoHeaders);
        clientUser1.send(liveCommand);
        final Adaptable liveCommandAdaptable = incomingMessages.take();
        assertThat(liveCommandAdaptable.getPayload().getExtra()).contains(expectedCounterAttributeIncrByTwo);
        assertThat(protocolAdapter.fromAdaptable(liveCommandAdaptable))
                .isInstanceOf(CreateThing.class)
                .extracting(s -> s.getDittoHeaders().getChannel())
                .isEqualTo(Optional.of(TopicPath.Channel.LIVE.getName()));

        // WHEN: clientUser1 sends live event
        // THEN: clientUser2 receives enriched live event
        final ThingCreated liveEvent = ThingCreated.of(thing, 123L, null, liveDittoHeaders, null);
        clientUser1.sendAdaptable(protocolAdapter.toAdaptable(liveEvent));
        final Adaptable liveEventAdaptable = incomingMessages.take();
        assertThat(liveEventAdaptable.getPayload().getExtra()).contains(expectedCounterAttributeIncrByTwo);
        assertThat(protocolAdapter.fromAdaptable(liveEventAdaptable))
                .isInstanceOf(ThingCreated.class)
                .extracting(s -> s.getDittoHeaders().getChannel())
                .isEqualTo(Optional.of(TopicPath.Channel.LIVE.getName()));
    }

    @Test
    public void subscribeForSignalsWithExtraFieldsContainingFeatureWildcard() throws Exception {
        final ThingId thingId = ThingId.of(idGenerator(testingContext1.getSolution().getDefaultNamespace()).withRandomName());
        final Policy policy = newPolicy(PolicyId.of(thingId), user1, user2);
        final JsonPointer counterPointer = JsonPointer.of("counter");
        final JsonValue counterValue = JsonValue.of(5725);
        final JsonValue counterValueIncrementedByOne = JsonValue.of(5726);
        final JsonValue counterValueIncrementedByTwo = JsonValue.of(5727);
        final JsonObject expectedCounterProperty = JsonObject.newBuilder()
                .set(JsonPointer.of("features/f1/properties").append(counterPointer), counterValue)
                .set(JsonPointer.of("features/f2/properties").append(counterPointer), counterValue)
                .build();
        final JsonObject expectedCounterPropertyIncrByOne = JsonObject.newBuilder()
                .set(JsonPointer.of("features/f1/properties").append(counterPointer), counterValueIncrementedByOne)
                .set(JsonPointer.of("features/f2/properties").append(counterPointer), counterValue)
                .build();
        final JsonObject expectedCounterPropertyIncrByTwo = JsonObject.newBuilder()
                .set(JsonPointer.of("features/f1/properties").append(counterPointer), counterValueIncrementedByTwo)
                .set(JsonPointer.of("features/f2/properties").append(counterPointer), counterValueIncrementedByOne)
                .build();

        final FeatureProperties featureProperties =
                ThingsModelFactory.newFeaturePropertiesBuilder()
                        .set("counter", counterValue)
                        .set("connected", true)
                        .build();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setFeature("f1", featureProperties)
                .setFeature("f2", featureProperties)
                .setFeature("f3", ThingsModelFactory.newFeaturePropertiesBuilder().set("connected", true).build())
                .build();

        final BlockingQueue<Adaptable> incomingMessages = new LinkedBlockingQueue<>();

        // clientUser2: push all incoming messages in a queue
        clientUser2.onAdaptable(incomingMessages::add);

        // GIVEN: clientUser2 subscribes to all signals with extra fields
        final String extraFields = "features/*/properties/counter";
        CompletableFuture.allOf(
                clientUser2.sendProtocolCommand("START-SEND-EVENTS?extraFields=" + extraFields,
                        "START-SEND-EVENTS:ACK"),
                clientUser2.sendProtocolCommand("START-SEND-MESSAGES?extraFields=" + extraFields,
                        "START-SEND-MESSAGES:ACK"),
                clientUser2.sendProtocolCommand("START-SEND-LIVE-EVENTS?extraFields=" + extraFields,
                        "START-SEND-LIVE-EVENTS:ACK"),
                clientUser2.sendProtocolCommand("START-SEND-LIVE-COMMANDS?extraFields=" + extraFields,
                        "START-SEND-LIVE-COMMANDS:ACK")
        ).join();

        // WHEN: clientUser1 sends twin command
        // THEN: clientUser2 receives enriched twin event
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), COMMAND_HEADERS_V2);
        clientUser1.send(createThing);
        final ProtocolAdapter protocolAdapter = DittoProtocolAdapter.newInstance();
        final Adaptable thingCreatedAdaptable = incomingMessages.take();
        assertThat(thingCreatedAdaptable.getPayload().getExtra()).contains(expectedCounterProperty);
        assertThat(protocolAdapter.fromAdaptable(thingCreatedAdaptable))
                .isInstanceOf(ThingCreated.class)
                .extracting(signal -> ((ThingCreated) signal).getEntityId())
                .isEqualTo(thingId);

        // WHEN: clientUser1 sends twin command modifying the enriched data to a more recent value
        // THEN: clientUser2 receives enriched twin event with the more recent value
        final ModifyFeatureProperty modifyProperty = ModifyFeatureProperty.of(thingId, "f1", counterPointer,
                JsonValue.of(counterValueIncrementedByOne), COMMAND_HEADERS_V2.toBuilder()
                        .randomCorrelationId()
                        .build());
        clientUser1.send(modifyProperty);
        final Adaptable propertyModifiedAdaptable = incomingMessages.take();
        assertThat(propertyModifiedAdaptable.getPayload().getExtra()).contains(expectedCounterPropertyIncrByOne);
        assertThat(protocolAdapter.fromAdaptable(propertyModifiedAdaptable))
                .isInstanceOf(FeaturePropertyModified.class)
                .extracting(signal -> ((FeaturePropertyModified) signal).getEntityId())
                .isEqualTo(thingId);

        // WHEN: clientUser1 sends twin merge command modifying the enriched data to a more recent value
        // THEN: clientUser2 receives enriched ThingMerge twin event with the more recent value
        final MergeThing mergeThing = MergeThing.of(thingId, JsonPointer.of("/features"),
                JsonObject.newBuilder()
                        .set(JsonPointer.of("f1/properties").append(counterPointer), counterValueIncrementedByTwo)
                        .set(JsonPointer.of("f2/properties").append(counterPointer), counterValueIncrementedByOne)
                        .build(), COMMAND_HEADERS_V2.toBuilder()
                        .randomCorrelationId()
                        .build());
        clientUser1.send(mergeThing);
        final Adaptable thingMergedAdaptable = incomingMessages.take();
        assertThat(thingMergedAdaptable.getPayload().getExtra()).contains(expectedCounterPropertyIncrByTwo);
        assertThat(protocolAdapter.fromAdaptable(thingMergedAdaptable))
                .isInstanceOf(ThingMerged.class)
                .extracting(signal -> ((ThingMerged) signal).getEntityId())
                .isEqualTo(thingId);

        // WHEN: clientUser1 sends live message
        // THEN: clientUser2 receives enriched live message
        final MessageHeaders messageHeaders =
                MessagesModelFactory.newHeadersBuilder(MessageDirection.TO, thingId, "JustSayingHello")
                        .contentType("text/plain")
                        .build();
        final String messagePayload = "Hello there, " + thingId;
        final Message<?> message = MessagesModelFactory.newMessageBuilder(messageHeaders)
                .payload(messagePayload)
                .build();
        clientUser1.send(SendThingMessage.of(thingId, message, COMMAND_HEADERS_V2.toBuilder()
                .randomCorrelationId()
                .build()));
        final Adaptable thingMessageAdaptable = incomingMessages.take();
        assertThat(thingMessageAdaptable.getPayload().getExtra()).contains(expectedCounterPropertyIncrByTwo);
        assertThat(protocolAdapter.fromAdaptable(thingMessageAdaptable))
                .isInstanceOf(SendThingMessage.class)
                .extracting(msg -> ((SendThingMessage<?>) msg).getMessage().getPayload())
                .isEqualTo(Optional.of(messagePayload));

        // WHEN: clientUser1 sends live command
        // THEN: clientUser2 receives enriched live command
        final DittoHeaders liveDittoHeaders = COMMAND_HEADERS_V2.toBuilder()
                .randomCorrelationId()
                .channel(TopicPath.Channel.LIVE.getName()).build();
        final CreateThing liveCommand = createThing.setDittoHeaders(liveDittoHeaders);
        clientUser1.send(liveCommand);
        final Adaptable liveCommandAdaptable = incomingMessages.take();
        assertThat(liveCommandAdaptable.getPayload().getExtra()).contains(expectedCounterPropertyIncrByTwo);
        assertThat(protocolAdapter.fromAdaptable(liveCommandAdaptable))
                .isInstanceOf(CreateThing.class)
                .extracting(s -> s.getDittoHeaders().getChannel())
                .isEqualTo(Optional.of(TopicPath.Channel.LIVE.getName()));

        // WHEN: clientUser1 sends live event
        // THEN: clientUser2 receives enriched live event
        final ThingCreated liveEvent = ThingCreated.of(thing, 123L, null, liveDittoHeaders, null);
        clientUser1.sendAdaptable(protocolAdapter.toAdaptable(liveEvent));
        final Adaptable liveEventAdaptable = incomingMessages.take();
        assertThat(liveEventAdaptable.getPayload().getExtra()).contains(expectedCounterPropertyIncrByTwo);
        assertThat(protocolAdapter.fromAdaptable(liveEventAdaptable))
                .isInstanceOf(ThingCreated.class)
                .extracting(s -> s.getDittoHeaders().getChannel())
                .isEqualTo(Optional.of(TopicPath.Channel.LIVE.getName()));
    }

    @Test
    @RunIf(DockerEnvironment.class)
    public void sendManyCommandsAndVerifyProcessingIsThrottled() throws Exception {

        final int totalMessages = 300;
        final Duration minDuration = Duration.ofMillis(2500L);
        final Duration maxDuration = Duration.ofSeconds(5L);

        // create new websocket connection to not influence other tests
        final AuthClient user1 = testingContext1.getOAuthClient();
        try (final ThingsWebsocketClient client = newTestWebsocketClient(user1.getAccessToken(), Map.of(),
                TestConstants.API_V_2)) {

            client.connect("ThingsWebsocketClient-toBeThrottled-" + UUID.randomUUID());

            final ThingId thingId = ThingId.of(idGenerator(testingContext1.getSolution().getDefaultNamespace()).withRandomName());
            final Thing thing = Thing.newBuilder().setId(thingId).build();
            final CreateThing createThing = CreateThing.of(thing, null, DittoHeaders.newBuilder()
                    .correlationId("createThing-throttled")
                    .build());

            clientUser1.send(createThing).whenComplete((commandResponse, throwable) -> {
                assertThat(throwable).isNull();
                assertThat(commandResponse).isInstanceOf(CreateThingResponse.class);
            }).get(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            final Instant startInstant = Instant.now();

            final CompletableFuture[] replies = IntStream.range(0, totalMessages)
                    .mapToObj(i ->
                            client.send(RetrieveThing.of(thingId, DittoHeaders.newBuilder()
                                    .correlationId("retrieveThing-throttled-" + i)
                                    .build()))
                    )
                    .toArray(CompletableFuture[]::new);

            CompletableFuture.allOf(replies).join();

            final Instant endInstant = Instant.now();
            final Duration totalDuration = Duration.between(startInstant, endInstant);

            LOGGER.info("THROTTLING-RESULT: <{}> request-response cycles took <{}>.", totalMessages, totalDuration);
            assertThat(totalDuration).isBetween(minDuration, maxDuration);

            // There should be no rejected requests. But if there were, their status should be TOO_MANY_REQUESTS.
            final long failures = Arrays.stream(replies)
                    .map(CompletableFuture::join)
                    .filter(response -> response instanceof ThingErrorResponse)
                    .map(response -> {
                        final DittoRuntimeException throwable =
                                ((ThingErrorResponse) response).getDittoRuntimeException();
                        assertThat(throwable.getHttpStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
                        return throwable;
                    })
                    .count();

            assertThat(failures)
                    .describedAs("rejected requests")
                    .isZero();
        }
    }

    @Test
    public void createThingRequestingAcknowledgementsPassesThroughToEvent() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);

        final AcknowledgementRequest acknowledgementRequest1 =
                AcknowledgementRequest.of(DittoAcknowledgementLabel.TWIN_PERSISTED);
        final AcknowledgementRequest acknowledgementRequest2 =
                AcknowledgementRequest.of(AcknowledgementLabel.of(requestedAckClient2));

        final JsonObject customAckPayload = JsonObject.newBuilder().set("custom", "payload").build();

        clientUser2.startConsumingEvents(event -> {
            assertThat(event).isInstanceOf(ThingCreated.class);
            // the Ditto internal ack requests must not be present:
            assertThat(event.getDittoHeaders().getAcknowledgementRequests()).doesNotContain(acknowledgementRequest1);
            assertThat(event.getDittoHeaders().getAcknowledgementRequests()).contains(acknowledgementRequest2);
            final Acknowledgement acknowledgement = Acknowledgement.of(acknowledgementRequest2.getLabel(),
                    ((ThingCreated) event).getEntityId(),
                    HttpStatus.RESET_CONTENT,
                    DittoHeaders.newBuilder().correlationId(event.getDittoHeaders().getCorrelationId().get()).build(),
                    customAckPayload);
            clientUser2.emit(acknowledgement);
            latch.countDown();
        }).join();

        final ThingId thingId = ThingId.of(idGenerator(testingContext1.getSolution().getDefaultNamespace()).withRandomName());
        final PolicyId policyId = PolicyId.of(thingId);
        final Policy policy = newPolicy(policyId, List.of(user1, user2), List.of(user1));
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .build();

        final DittoHeaders dittoHeaders = COMMAND_HEADERS_V2.toBuilder()
                .acknowledgementRequest(acknowledgementRequest1, acknowledgementRequest2)
                .build();
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), dittoHeaders);

        final CountDownLatch responseLatch = new CountDownLatch(1);
        clientUser1.send(createThing).whenComplete((commandResponse, throwable) -> {
            assertThat(throwable).isNull();
            assertThat(commandResponse).isInstanceOf(Acknowledgements.class);
            final Acknowledgements acks = (Acknowledgements) commandResponse;
            LOGGER.info("Got Acknowledgements: {}", acks);
            assertThat(acks.getHttpStatus()).isEqualTo(HttpStatus.OK);
            assertThat(acks.getSize()).isEqualTo(2);
            final Set<Acknowledgement> successfulAcknowledgements = acks.getSuccessfulAcknowledgements();
            assertThat(successfulAcknowledgements.stream()
                    .map(Acknowledgement::getLabel)
            ).contains(acknowledgementRequest1.getLabel(), acknowledgementRequest2.getLabel());

            assertThat(filterByLabel(successfulAcknowledgements, acknowledgementRequest1.getLabel())
                    .map(Acknowledgement::getHttpStatus)
                    .findAny()
            ).contains(HttpStatus.CREATED);
            assertThat(filterByLabel(successfulAcknowledgements, acknowledgementRequest1.getLabel())
                    .map(Acknowledgement::getEntity)
                    .flatMap(Optional::stream)
                    .map(JsonValue::asObject)
                    .map(ThingsModelFactory::newThing)
                    .findAny()
            ).contains(thing);

            assertThat(filterByLabel(successfulAcknowledgements, acknowledgementRequest2.getLabel())
                    .map(Acknowledgement::getHttpStatus)
                    .findAny()
            ).contains(HttpStatus.RESET_CONTENT);
            assertThat(filterByLabel(successfulAcknowledgements, acknowledgementRequest2.getLabel())
                    .map(Acknowledgement::getEntity)
                    .flatMap(Optional::stream)
                    .findAny()
            ).contains(customAckPayload);

            responseLatch.countDown();
        }).join();

        assertThat(latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(responseLatch.await(LATCH_TIMEOUT_SECONDS * 2, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    @Category(Acceptance.class)
    public void weakAcksAreIssuedForFilteredEvents() {

        final AcknowledgementRequest acknowledgementRequest1 =
                AcknowledgementRequest.of(AcknowledgementLabel.of(declaredAckClient1));
        final AcknowledgementRequest acknowledgementRequest2 =
                AcknowledgementRequest.of(AcknowledgementLabel.of(requestedAckClient2));

        // Required to consume events, otherwise the service issues a weak ack for declaredAckClient1
        clientUser1.startConsumingEvents(event -> {}).join();
        //This is just a random RQL filter which definitely does not match the created thing
        clientUser2.startConsumingEvents(event -> {}, "gt(attributes/counter,10)").join();

        final ThingId thingId = ThingId.of(idGenerator(testingContext1.getSolution().getDefaultNamespace()).withRandomName());
        final Policy policy = newPolicy(PolicyId.of(thingId), List.of(user1, user2), List.of(user1));
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();

        final DittoHeaders dittoHeaders = COMMAND_HEADERS_V2.toBuilder()
                .acknowledgementRequest(acknowledgementRequest1, acknowledgementRequest2)
                .timeout(Duration.ofSeconds(2))
                .build();
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), dittoHeaders);

        clientUser1.send(createThing).handle((commandResponse, throwable) -> {
            assertThat(throwable).isNull();
            assertThat(commandResponse).isInstanceOf(Acknowledgements.class);
            final Acknowledgements acks = (Acknowledgements) commandResponse;
            LOGGER.info("Got Acknowledgements: {}", acks);
            assertThat(acks.getHttpStatus()).isEqualTo(HttpStatus.FAILED_DEPENDENCY);
            assertThat(acks.getSize()).isEqualTo(2);
            final Set<Acknowledgement> successfulAcknowledgements = acks.getSuccessfulAcknowledgements();
            assertThat(successfulAcknowledgements.stream()
                    .map(Acknowledgement::getLabel)
            ).containsExactly(acknowledgementRequest2.getLabel());

            assertThat(filterByLabel(successfulAcknowledgements, acknowledgementRequest2.getLabel())
                    .map(Acknowledgement::isWeak)
                    .findAny()
            ).contains(true);

            final Set<Acknowledgement> failedAcknowledgements = acks.getFailedAcknowledgements();
            assertThat(failedAcknowledgements.stream()
                    .map(Acknowledgement::getLabel)
            ).containsExactly(acknowledgementRequest1.getLabel());

            assertThat(filterByLabel(failedAcknowledgements, acknowledgementRequest1.getLabel())
                    .map(Acknowledgement::getHttpStatus)
                    .findAny()
            ).contains(HttpStatus.REQUEST_TIMEOUT);

            return commandResponse;
        }).join();
    }

    @Test
    @Category(Acceptance.class)
    public void weakAcksAreIssuedForUnauthorizedSubscribers() {

        final AcknowledgementRequest requestUnauthorizedSubscriber =
                AcknowledgementRequest.of(AcknowledgementLabel.of(requestedAckClient2));
        final AcknowledgementRequest requestNonexistentSubscriber =
                AcknowledgementRequest.parseAcknowledgementRequest("request-nonexistent-subscriber");

        clientUser2.startConsumingEvents(event -> {}).join();

        // wait for another round of gossip to ensure replication of declared acks
        clientUser1.startConsumingEvents(event -> {}).join();

        final ThingId thingId = ThingId.of(idGenerator(testingContext1.getSolution().getDefaultNamespace()).withRandomName());
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();

        final DittoHeaders dittoHeaders = COMMAND_HEADERS_V2.toBuilder()
                .acknowledgementRequest(requestUnauthorizedSubscriber, requestNonexistentSubscriber)
                .timeout(Duration.ofSeconds(2))
                .build();
        final CreateThing createThing = CreateThing.of(thing, null, dittoHeaders);

        clientUser1.send(createThing).whenComplete((commandResponse, throwable) -> {
            assertThat(throwable).isNull();
            assertThat(commandResponse).isInstanceOf(Acknowledgements.class);
            final Acknowledgements acks = (Acknowledgements) commandResponse;
            LOGGER.info("Got Acknowledgements: {}", acks);
            assertThat(acks.getHttpStatus()).isEqualTo(HttpStatus.FAILED_DEPENDENCY);
            assertThat(acks.getSize()).isEqualTo(2);
            final Acknowledgement unauthorized =
                    acks.getAcknowledgement(requestUnauthorizedSubscriber.getLabel()).orElseThrow();
            final Acknowledgement nonexistent =
                    acks.getAcknowledgement(requestNonexistentSubscriber.getLabel()).orElseThrow();
            assertThat(unauthorized.isWeak())
                    .describedAs("Expect weak ack from unauthorized subscriber: " + unauthorized)
                    .isTrue();
            assertThat(nonexistent)
                    .describedAs("Expect timeout from nonexistent subscriber: " + nonexistent)
                    .satisfies(ack -> assertThat(ack.isWeak()).isFalse())
                    .satisfies(ack -> assertThat(ack.getHttpStatus()).isEqualTo(HttpStatus.REQUEST_TIMEOUT));
        }).join();
    }

    @Test
    public void subscribeForPolicyAnnouncement() throws Exception {
        // GIVEN: clientUser5 is subscribed for policy announcements
        final var subscribeMessage = "START-SEND-POLICY-ANNOUNCEMENTS";
        final BlockingQueue<Signal<?>> clientUser5ReceivedSignals = new LinkedBlockingQueue<>();
        clientUser5.onSignal(clientUser5ReceivedSignals::add);
        clientUser5.sendProtocolCommand(subscribeMessage, subscribeMessage + ":ACK").join();

        // WHEN: clientUser4 creates policy containing clientUser5's subject with expiry and imminent announcement
        final var policyId = PolicyId.inNamespaceWithRandomName(
                serviceEnv.getTestingContext4().getSolution().getDefaultNamespace());
        final var subject5 = serviceEnv.getTestingContext5().getOAuthClient().getDefaultSubject();
        final var subject5Expiry = SubjectExpiry.newInstance(Instant.now().plus(Duration.ofSeconds(3600)));
        final var subject5Announcement = SubjectAnnouncement.of(DittoDuration.parseDuration("3599s"), true);
        final var subject5WithExpiry =
                Subject.newInstance(subject5.getId(), SubjectType.GENERATED, subject5Expiry, subject5Announcement);
        final var subject5WhenDeleted =
                Subject.newInstance(subject5.getId(), SubjectType.GENERATED, null, subject5Announcement);
        final var policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("user4")
                .setSubject(serviceEnv.getTestingContext4().getOAuthClient().getDefaultSubject())
                .setSubject(testingContext1.getOAuthClient().getDefaultSubject())
                .setResource(Resource.newInstance("policy", "/",
                        EffectedPermissions.newInstance(List.of("READ", "WRITE"), List.of())))
                .forLabel("user5")
                .setSubject(subject5WithExpiry)
                .setResource(Resource.newInstance("policy", "/",
                        EffectedPermissions.newInstance(List.of("EXECUTE"), List.of())))
                .build();
        rememberForCleanUp(deletePolicy(policyId)
                .withJWT(serviceEnv.getTestingContext4().getOAuthClient().getAccessToken()));
        final var createResponse = clientUser4.send(CreatePolicy.of(policy, DittoHeaders.newBuilder()
                .randomCorrelationId()
                .build())).join();
        assertThat(createResponse).isInstanceOf(CreatePolicyResponse.class);

        // THEN: clientUser5 receives a SubjectDeletionAnnouncement.
        final var expiryAnnouncement =
                (SubjectDeletionAnnouncement) clientUser5ReceivedSignals.poll(15, TimeUnit.SECONDS);
        assertThat(expiryAnnouncement)
                .describedAs("Did not receive announcement before subject expiry.")
                .isNotNull();
        assertThat(expiryAnnouncement.getSubjectIds()).containsExactly(subject5.getId());

        // GIVEN: clientUser5's subject is refreshed
        putPolicyEntrySubject(policyId, "user5", subject5.getId().toString(), subject5WhenDeleted)
                .withJWT(serviceEnv.getTestingContext4().getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // WHEN: clientUser4 deletes the policy before expiry of clientUser5's subject
        final var deleteResponse = clientUser4.send(
                DeletePolicy.of(policyId, DittoHeaders.newBuilder().randomCorrelationId().build())).join();
        assertThat(deleteResponse).isInstanceOf(DeletePolicyResponse.class);

        // THEN: clientUser5 receives notification about its subject's deletion
        final var deletionAnnouncement =
                (SubjectDeletionAnnouncement) clientUser5ReceivedSignals.poll(15, TimeUnit.SECONDS);
        assertThat(deletionAnnouncement)
                .describedAs("Did not receive announcement on subject deletion.")
                .isNotNull();
        assertThat(deletionAnnouncement.getSubjectIds()).containsExactly(subject5.getId());
    }

    @Test
    public void subscribeForPolicyAnnouncementWithAcknowledgement() throws Exception {
        // GIVEN: clientUser2 is subscribed for policy announcements
        final var subscribeMessage = "START-SEND-POLICY-ANNOUNCEMENTS";
        final BlockingQueue<Signal<?>> clientUser2ReceivedSignals = new LinkedBlockingQueue<>();
        clientUser2.onSignal(clientUser2ReceivedSignals::add);
        clientUser2.sendProtocolCommand(subscribeMessage, subscribeMessage + ":ACK").join();

        // WHEN: a policy is created containing clientUser2's subject with expiry and imminent announcement
        final var policyId = PolicyId.inNamespaceWithRandomName(testingContext1
                .getSolution().getDefaultNamespace());
        final AcknowledgementLabel client2Label = AcknowledgementLabel.of(declaredAckClient2);
        final var subject2 = testingContext2.getOAuthClient().getDefaultSubject();
        final var subject2Expiry = SubjectExpiry.newInstance(Instant.now().plus(Duration.ofSeconds(3)));
        final var subject2Announcement =
                SubjectAnnouncement.of(DittoDuration.parseDuration("5s"), true,
                        List.of(AcknowledgementRequest.of(client2Label)),
                        DittoDuration.parseDuration("60s"), null);
        final var subject2WithExpiry =
                Subject.newInstance(subject2.getId(), SubjectType.GENERATED, subject2Expiry, subject2Announcement);
        final var label = "user2";
        final var policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("user1")
                .setSubject(testingContext1.getOAuthClient().getDefaultSubject())
                .setResource(Resource.newInstance("policy", "/",
                        EffectedPermissions.newInstance(List.of("READ", "WRITE"), List.of())))
                .forLabel(label)
                .setSubject(subject2WithExpiry)
                .setResource(Resource.newInstance("policy", "/",
                        EffectedPermissions.newInstance(List.of("EXECUTE"), List.of())))
                .build();

        putPolicy(policy)
                .withJWT(testingContext1.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.CREATED).fire();

        // WHEN: clientUser2's subject expired
        TimeUnit.SECONDS.sleep(5);

        // THEN: clientUser2 receives a SubjectDeletionAnnouncement.
        final var expiryAnnouncement =
                (SubjectDeletionAnnouncement) clientUser2ReceivedSignals.poll(15, TimeUnit.SECONDS);
        assertThat(expiryAnnouncement)
                .describedAs("Did not receive announcement before subject expiry.")
                .isNotNull();
        assertThat(expiryAnnouncement.getSubjectIds()).containsExactly(subject2.getId());
        assertThat(expiryAnnouncement.getDittoHeaders().getAcknowledgementRequests())
                .containsExactly(AcknowledgementRequest.of(client2Label));
        assertExpiredSubjectNotDeleted(policyId, label, subject2.getId());

        // WHEN: clientUser2 replies with a negative acknowledgement
        final ProtocolAdapter protocolAdapter = DittoProtocolAdapter.newInstance();
        final Acknowledgement nack = Acknowledgement.of(client2Label, policyId, HttpStatus.REQUEST_TIMEOUT,
                expiryAnnouncement.getDittoHeaders());
        clientUser2.sendWithoutResponse(
                ProtocolFactory.wrapAsJsonifiableAdaptable(protocolAdapter.toAdaptable(nack)).toJsonString()
        );

        // THEN: clientUser2 receives a SubjectDeletionAnnouncement again
        final var resentAnnouncement =
                (SubjectDeletionAnnouncement) clientUser2ReceivedSignals.poll(15, TimeUnit.SECONDS);
        assertThat(resentAnnouncement)
                .describedAs("Did not receive announcement on subject deletion.")
                .isNotNull();
        assertThat(resentAnnouncement.getSubjectIds()).containsExactly(subject2.getId());
        assertExpiredSubjectNotDeleted(policyId, label, subject2.getId());

        // WHEN: clientUser2 replies with a positive acknowledgement
        final Acknowledgement ack = Acknowledgement.of(client2Label, policyId, HttpStatus.OK,
                resentAnnouncement.getDittoHeaders());
        clientUser2.sendWithoutResponse(
                ProtocolFactory.wrapAsJsonifiableAdaptable(protocolAdapter.toAdaptable(ack)).toJsonString()
        );

        // THEN: entry is deleted
        getPolicyEntrySubject(policyId, label, subject2.getId().toString())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .useAwaitility(AWAITILITY_DEFAULT_CONFIG)
                .fire();
    }

    private void assertExpiredSubjectNotDeleted(final PolicyId policyId, final String label,
            final SubjectId subjectId) {
        getPolicyEntrySubject(policyId, label, subjectId.toString())
                .withJWT(testingContext1.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    private static Stream<Acknowledgement> filterByLabel(final Set<Acknowledgement> successfulAcknowledgements,
            final AcknowledgementLabel label) {

        return successfulAcknowledgements.stream().filter(ack -> ack.getLabel().equals(label));
    }

    private DittoHeaders commandHeadersWithOwnCorrelationId() {
        return COMMAND_HEADERS_V2.toBuilder()
                .correlationId(UUID.randomUUID().toString())
                .build();
    }
}
