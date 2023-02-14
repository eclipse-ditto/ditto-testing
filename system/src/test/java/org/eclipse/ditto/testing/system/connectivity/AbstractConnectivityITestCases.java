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

import static io.restassured.RestAssured.expect;
import static org.assertj.core.api.Assertions.fail;
import static org.eclipse.ditto.json.assertions.DittoJsonAssertions.assertThat;
import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION1;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION2;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION_WITH_2_SOURCES;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION_WITH_CONNECTION_ANNOUNCEMENTS;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION_WITH_ENFORCEMENT_ENABLED;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION_WITH_EXTRA_FIELDS;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION_WITH_NAMESPACE_AND_RQL_FILTER;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.NONE;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.acks.AcknowledgementLabelNotDeclaredException;
import org.eclipse.ditto.base.model.acks.AcknowledgementRequest;
import org.eclipse.ditto.base.model.acks.DittoAcknowledgementLabel;
import org.eclipse.ditto.base.model.common.DittoDuration;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.entity.id.EntityId;
import org.eclipse.ditto.base.model.entity.id.WithEntityId;
import org.eclipse.ditto.base.model.exceptions.DittoRuntimeException;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.json.Jsonifiable;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgement;
import org.eclipse.ditto.base.model.signals.acks.Acknowledgements;
import org.eclipse.ditto.base.model.signals.commands.CommandResponse;
import org.eclipse.ditto.base.model.signals.commands.ErrorResponse;
import org.eclipse.ditto.base.model.signals.commands.exceptions.CommandTimeoutException;
import org.eclipse.ditto.base.model.signals.commands.streaming.RequestFromStreamingSubscription;
import org.eclipse.ditto.base.model.signals.commands.streaming.SubscribeForPersistedEvents;
import org.eclipse.ditto.base.model.signals.events.streaming.StreamingSubscriptionComplete;
import org.eclipse.ditto.base.model.signals.events.streaming.StreamingSubscriptionCreated;
import org.eclipse.ditto.base.model.signals.events.streaming.StreamingSubscriptionHasNext;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.FilteredTopic;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.connectivity.model.Topic;
import org.eclipse.ditto.connectivity.model.signals.announcements.ConnectionClosedAnnouncement;
import org.eclipse.ditto.connectivity.model.signals.announcements.ConnectionOpenedAnnouncement;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonKey;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.jwt.model.ImmutableJsonWebToken;
import org.eclipse.ditto.jwt.model.JsonWebToken;
import org.eclipse.ditto.messages.model.Message;
import org.eclipse.ditto.messages.model.MessageBuilder;
import org.eclipse.ditto.messages.model.MessageDirection;
import org.eclipse.ditto.messages.model.MessageHeaders;
import org.eclipse.ditto.messages.model.signals.commands.MessageCommand;
import org.eclipse.ditto.messages.model.signals.commands.SendFeatureMessage;
import org.eclipse.ditto.messages.model.signals.commands.SendFeatureMessageResponse;
import org.eclipse.ditto.messages.model.signals.commands.SendThingMessage;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.Permissions;
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
import org.eclipse.ditto.policies.model.signals.commands.PolicyErrorResponse;
import org.eclipse.ditto.policies.model.signals.commands.modify.CreatePolicy;
import org.eclipse.ditto.policies.model.signals.commands.modify.CreatePolicyResponse;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.protocol.TopicPath;
import org.eclipse.ditto.protocol.adapter.DittoProtocolAdapter;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.ThingJsonProducer;
import org.eclipse.ditto.testing.common.ThingsSubjectIssuer;
import org.eclipse.ditto.testing.common.categories.RequireSource;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Features;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.eclipse.ditto.things.model.signals.commands.ThingErrorResponse;
import org.eclipse.ditto.things.model.signals.commands.exceptions.ThingConditionFailedException;
import org.eclipse.ditto.things.model.signals.commands.exceptions.ThingConditionInvalidException;
import org.eclipse.ditto.things.model.signals.commands.exceptions.ThingNotCreatableException;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThingResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.DeleteThing;
import org.eclipse.ditto.things.model.signals.commands.modify.MergeThing;
import org.eclipse.ditto.things.model.signals.commands.modify.MergeThingResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttribute;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttributeResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyFeature;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyFeatureProperty;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThings;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThingsResponse;
import org.eclipse.ditto.things.model.signals.events.AttributeCreated;
import org.eclipse.ditto.things.model.signals.events.AttributeModified;
import org.eclipse.ditto.things.model.signals.events.ThingCreated;
import org.eclipse.ditto.things.model.signals.events.ThingDeleted;
import org.eclipse.ditto.things.model.signals.events.ThingEvent;
import org.eclipse.ditto.things.model.signals.events.ThingMerged;
import org.eclipse.ditto.things.model.signals.events.ThingModified;
import org.eclipse.ditto.thingsearch.model.signals.commands.subscription.CreateSubscription;
import org.eclipse.ditto.thingsearch.model.signals.commands.subscription.RequestFromSubscription;
import org.eclipse.ditto.thingsearch.model.signals.events.SubscriptionCreated;
import org.eclipse.ditto.thingsearch.model.signals.events.SubscriptionFailed;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.response.Response;

/**
 * Integration test cases for Connectivity testing - implemented by concrete Connectivity types.
 *
 * @param <C> the consumer
 * @param <M> the consumer message
 */
public abstract class AbstractConnectivityITestCases<C, M> extends
        AbstractConnectivityTunnelingITestCases<C, M> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnectivityITestCases.class);

    private static final String EMPTY_MOD = "EMPTY_MOD";
    private static final String FILTER_FOR_LIFECYCLE_EVENTS_BY_RQL = "FILTER_FOR_LIFECYCLE_EVENTS_BY_RQL";

    static {
        // adds filtering for twin events - only "created" and "deleted" ones
        addMod(FILTER_FOR_LIFECYCLE_EVENTS_BY_RQL, connection -> {
                    final List<Target> adjustedTargets =
                            connection.getTargets().stream()
                                    .map(target -> {
                                        final Set<FilteredTopic> topics = new HashSet<>();
                                        // only subscribe for live messages
                                        topics.add(ConnectivityModelFactory
                                                .newFilteredTopicBuilder(Topic.TWIN_EVENTS)
                                                .withFilter("and(" +
                                                            "in(topic:action,'created','deleted')," +
                                                            "eq(resource:path,'/')" +
                                                        ")")
                                                .build()
                                        );
                                        return ConnectivityModelFactory.newTargetBuilder(target)
                                                .topics(topics)
                                                .build();
                                    })
                                    .collect(Collectors.toList());
                    return connection.toBuilder()
                            .setTargets(adjustedTargets)
                            .build();
                }
        );
        addMod(EMPTY_MOD, Function.identity());
    }

    protected AbstractConnectivityITestCases(final ConnectivityFactory cf) {
        super(cf);
    }

    @Test
    @Category(RequireSource.class)
    @Connections(CONNECTION1)
    public void sendCreateThingAndEnsureResponseIsSentBack() {
        sendCreateThingAndEnsureResponseIsSentBack(cf.connectionName1);
    }

    @Test
    @Category(RequireSource.class)
    @Connections(CONNECTION_WITH_2_SOURCES)
    public void consumeMessageOnlyOnRightTopic() {
        final var connection = cf.connectionWith2Sources;
        final CreateThing createThing = buildCreateThing(connection, ThingBuilder.FromScratch::build);
        final ThingId thingEntityId = createThing.getEntityId();

        // Given
        final String correlationId =
                createThing.getDittoHeaders().getCorrelationId()
                        .orElseThrow(() -> new IllegalStateException("correlation-id is required."));

        // When
        final C consumer = initResponseConsumer(cf.connectionWith2Sources, correlationId);
        sendSignal(cf.connectionWith2Sources + ConnectionModelFactory.SOURCE1_SUFFIX, createThing);

        // Then
        final M message = consumeResponse(correlationId, consumer);
        LOGGER.info("Received expected response message: {}", message);
        // Connection should only get response via one source.
        try {
            final M message2 = consumeResponse(correlationId, consumer);
            if (null != message2) {
                LOGGER.error("Received unexpected response message: {}", message);
            }
            assertThat(message2).isNull();
        } catch (final Exception e){
            assertThat(e).isNotNull();
        }

        assertThat(message).isNotNull();
        assertThat(getCorrelationId(message)).isEqualTo(correlationId);
        checkHeaders(message);

        final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(message);
        final Adaptable responseAdaptable =
                ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
        final Signal<?> response = PROTOCOL_ADAPTER.fromAdaptable(responseAdaptable);

        assertThat(response).isNotNull();
        assertThat(response).isInstanceOf(CommandResponse.class);
        assertThat(response).isInstanceOf(CreateThingResponse.class);
        assertThat(((CreateThingResponse)response).getHttpStatus()).isEqualTo(HttpStatus.CREATED);
        assertThat(((CreateThingResponse) response).getThingCreated()
                .flatMap(Thing::getEntityId))
                .contains(thingEntityId);
    }

    @Test
    @Connections(CONNECTION1)
    public void receiveSubjectDeletionNotification() {
        final var policyId = PolicyId.inNamespaceWithRandomName(serviceEnv.getDefaultNamespaceName());
        final var subjectId =
                SubjectId.newInstance(
                        connectionAuthIdentifier(testingContextWithRandomNs.getSolution().getUsername(),
                                TestingContext.DEFAULT_SCOPE));
        final var subjectExpiry = SubjectExpiry.newInstance(Instant.now().plus(Duration.ofSeconds(3600)));
        final var subjectAnnouncement = SubjectAnnouncement.of(DittoDuration.parseDuration("3599s"), true);
        final var subject =
                Subject.newInstance(subjectId, SubjectType.GENERATED, subjectExpiry, subjectAnnouncement);
        final var subjectForDeletion =
                Subject.newInstance(subjectId, SubjectType.GENERATED, null, subjectAnnouncement);
        final var policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("user1")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setResource(Resource.newInstance("policy", "/",
                        EffectedPermissions.newInstance(List.of("READ", "WRITE"), List.of())))
                .forLabel("connection1")
                .setSubject(subject)
                .setResource(Resource.newInstance("thing", "/",
                        EffectedPermissions.newInstance(List.of("READ"), List.of())))
                .build();

        final C consumer = initTargetsConsumer(cf.connectionName1);
        LOGGER.info("Creating Policy with subjectExpiry: <{}>", policy);
        putPolicy(policy).expectingHttpStatus(HttpStatus.CREATED).fire();
        LOGGER.info("Expecting to receive expiry announcement ...");
        final var expiryAnnouncement = expectMsgClass(SubjectDeletionAnnouncement.class, cf.connectionName1, consumer);
        assertThat(expiryAnnouncement).as("Expiry announcement was not received!")
                .isNotNull();
        assertThat(expiryAnnouncement.getSubjectIds()).containsExactly(subjectId);

        // modify subject to publish announcement when deleted
        putPolicyEntrySubject(policyId, "connection1", subjectId.toString(), subjectForDeletion)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        deletePolicy(policyId).expectingHttpStatus(HttpStatus.NO_CONTENT).fire();
        final var deleteAnnouncement = expectMsgClass(SubjectDeletionAnnouncement.class, cf.connectionName1, consumer);
        assertThat(deleteAnnouncement.getSubjectIds()).containsExactly(subjectId);
    }

    @Test
    @Connections(CONNECTION1)
    public void receiveSubjectDeletionNotificationAfterActivatingTokenIntegration() {
        final var policyId = PolicyId.inNamespaceWithRandomName(serviceEnv.getDefaultNamespaceName());

        final var policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("user")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setResource(Resource.newInstance("policy", "/",
                        EffectedPermissions.newInstance(List.of("READ", "WRITE", "EXECUTE"), List.of())))
                .forLabel("connection-target")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setResource(Resource.newInstance("thing", "/",
                        EffectedPermissions.newInstance(List.of("READ"), List.of())))
                .build();

        final C consumer = initTargetsConsumer(cf.connectionName1);
        putPolicy(policy).expectingHttpStatus(HttpStatus.CREATED).fire();

        final JsonWebToken jwt = ImmutableJsonWebToken.fromToken(
                testingContextWithRandomNs.getOAuthClient().getAccessToken());
        final Instant now = Instant.now();
        final Instant jwtExpirationTime = jwt.getExpirationTime();
        // system-tests are configured via policies env variable POLICY_SUBJECT_EXPIRY_GRANULARITY=5s, so also round up:
        final Instant roundedJwtExpirationTime = roundUpInstant(jwtExpirationTime, Duration.ofSeconds(5));
        LOGGER.info("Now: <{}> - jwtExpiry: <{}> - roundedJwtExpiry: <{}>", now, jwtExpirationTime,
                roundedJwtExpirationTime);

        final Duration durationUntilExpiry = Duration.between(now, roundedJwtExpirationTime)
                .minusSeconds(1); // announce "now + 1s"
        final DittoDuration announcementDuration = DittoDuration.parseDuration("1s").setAmount(durationUntilExpiry);
        final Instant predicatedAnnounceAt = roundedJwtExpirationTime.minus(announcementDuration.getDuration());
        final Duration predicatedDuration = Duration.between(now, predicatedAnnounceAt);
        LOGGER.info("Announcement will be at: <{}> (in: <{}>), durationUntilExpiry was: <{}>", predicatedAnnounceAt,
                predicatedDuration, durationUntilExpiry);
        final var subjectAnnouncement = SubjectAnnouncement.of(announcementDuration, false);
        final JsonObject payload = JsonObject.newBuilder()
                .set("announcement", subjectAnnouncement.toJson())
                .build();
        postPolicyAction(policyId, "activateTokenIntegration", payload.toString())
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final var connectionSubjectId =
                SubjectId.newInstance(connectionAuthIdentifier(testingContextWithRandomNs.getSolution().getUsername(),
                        TestingContext.DEFAULT_SCOPE));

        getPolicyEntrySubject(policyId, "connection-target", connectionSubjectId.toString())
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.OK)
                .expectingBody(contains(JsonKey.of("expiry"), JsonKey.of("announcement")))
                .fire();

        final var expiryAnnouncement = expectMsgClass(SubjectDeletionAnnouncement.class, cf.connectionName1, consumer);
        assertThat(expiryAnnouncement.getSubjectIds()).containsExactly(connectionSubjectId);
    }

    @Test
    @Category(RequireSource.class)
    @Connections(CONNECTION2)
    public void resendsAnnouncementIfNotAcked() throws Exception {
        // start consumer
        final var consumer = initTargetsConsumer(cf.connectionName2);

        // GIVEN: a policy is created with a expiring subject expiring soon
        final var ackLabel = connectionScopedAckLabel(cf.connectionName2, "custom", cf);
        final var ackTimeout = DittoDuration.parseDuration("60s");
        final var policyId =
                createExpiringPolicy(AcknowledgementRequest.of(ackLabel), ackTimeout, cf.connectionName2,
                        Duration.ofSeconds(3), DittoDuration.parseDuration("5s"));
        final var connectionSubjectId = connectionSubject(cf.connectionName2).getId();

        // WHEN: subject expired
        TimeUnit.SECONDS.sleep(3);

        // THEN: announcement was sent
        final var message1 = consumeFromTarget(cf.connectionName2, consumer);
        assertThat(message1).withFailMessage("Did not receive policy announcement 1").isNotNull();
        final var announcement1 = assertSubjectDeletionAnnouncement(message1, policyId, connectionSubjectId);

        // THEN: expired subject remains
        final var willGetDeleted = "will-get-deleted";
        assertExpiredSubjectNotDeleted(policyId, connectionSubjectId);

        // WHEN: announcement is acknowledged negatively
        final var nack = Acknowledgement.of(ackLabel, policyId, HttpStatus.INTERNAL_SERVER_ERROR,
                announcement1.getDittoHeaders());
        sendSignal(cf.connectionName2, nack);

        // wait for back-off
        TimeUnit.SECONDS.sleep(7);
        final var message2 = consumeFromTarget(cf.connectionName2, consumer);
        assertThat(message2).withFailMessage("Did not receive policy announcement 2").isNotNull();
        final var announcement2 = assertSubjectDeletionAnnouncement(message2, policyId, connectionSubjectId);

        assertExpiredSubjectNotDeleted(policyId, connectionSubjectId);

        final var ack = Acknowledgement.of(ackLabel, policyId, HttpStatus.OK, announcement2.getDittoHeaders());
        sendSignal(cf.connectionName2, ack);

        getPolicyEntrySubject(policyId, willGetDeleted, connectionSubjectId.toString())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .useAwaitility(AWAITILITY_DEFAULT_CONFIG)
                .fire();
    }

    protected void assertExpiredSubjectNotDeleted(final PolicyId policyId, final SubjectId subjectId) {
        final var policy = PoliciesModelFactory.newPolicy(
                getPolicy(policyId).expectingHttpStatus(HttpStatus.OK).fire().body().asString());
        assertThat(policy.getEntryFor("will-get-deleted")
                .flatMap(entry -> entry.getSubjects().getSubject(subjectId)))
                .describedAs("subject deleted before acknowledgement")
                .isNotEmpty();
    }

    protected SubjectDeletionAnnouncement assertSubjectDeletionAnnouncement(final M message, final PolicyId policyId,
            final SubjectId connectionSubjectId) {

        final var signal = PROTOCOL_ADAPTER.fromAdaptable(
                ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptableFrom(message)).build());
        assertThat(signal).isInstanceOf(SubjectDeletionAnnouncement.class);
        final var announcement = (SubjectDeletionAnnouncement) signal;
        assertThat(announcement.getEntityId().toString()).isEqualTo(policyId.toString());
        assertThat(announcement.getSubjectIds()).containsExactly(connectionSubjectId);
        return announcement;
    }

    protected PolicyId createExpiringPolicy(final AcknowledgementRequest ackRequest, final DittoDuration ackTimeout,
            final String connectionName, final Duration expiry, final DittoDuration sendAnnouncementBefore) {

        final var policyId = PolicyId.inNamespaceWithRandomName(serviceEnv.getDefaultNamespaceName());
        final var defaultSubject = testingContextWithRandomNs.getOAuthClient().getDefaultSubject();
        final var connectionSubjectId = connectionSubject(connectionName).getId();

        final var now = Instant.now();
        final var expirationTimestamp = now.plus(expiry);

        LOGGER.info("Now: <{}> - expirationTimestamp: <{}>", now, expirationTimestamp);

        final var subjectAnnouncement =
                SubjectAnnouncement.of(sendAnnouncementBefore, true, List.of(ackRequest), ackTimeout, null);
        final var policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("user")
                .setSubject(serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject())
                .setSubject(defaultSubject)
                .setResource(Resource.newInstance("policy", "/",
                        EffectedPermissions.newInstance(List.of("READ", "WRITE", "EXECUTE"), List.of())))
                .forLabel("will-get-deleted")
                .setSubject(Subject.newInstance(
                        connectionSubjectId,
                        SubjectType.newInstance("the-expiring-one"),
                        SubjectExpiry.newInstance(expirationTimestamp),
                        subjectAnnouncement))
                .build();

        LOGGER.info("createExpiringPolicy: {}", policy);
        putPolicy(policy).expectingHttpStatus(HttpStatus.CREATED).fire();

        return policyId;
    }

    private static Instant roundUpInstant(final Instant timestamp, final Duration granularity) {
        final Instant truncated = timestamp.truncatedTo(ChronoUnit.SECONDS);
        final Instant truncatedParent = timestamp.truncatedTo(ChronoUnit.MINUTES);

        final long amountBetween = ChronoUnit.SECONDS.between(truncatedParent, truncated);
        final long deltaModulo = amountBetween % granularity.getSeconds();

        if (truncated.equals(timestamp) && deltaModulo == 0) {
            // shortcut, when truncating leads to the given expiry timestamp, don't adjust the subjectExpiry at all:
            return timestamp;
        }

        final long toAdd = granularity.getSeconds() - deltaModulo;
        return truncated.plus(toAdd, ChronoUnit.SECONDS);
    }

    protected final void testSearchProtocol(final String connectionName, final CreateSubscription subscriptionCommand) {
        final String correlationId =
                subscriptionCommand.getDittoHeaders().getCorrelationId()
                        .orElseThrow(() -> new IllegalStateException("correlation-id is required."));
        final DittoHeaders dittoHeaders = subscriptionCommand.getDittoHeaders();

        final C eventConsumer = initResponseConsumer(connectionName, correlationId);
        sendSignal(connectionName, subscriptionCommand);
        consumeAndAssertEvents(connectionName, eventConsumer, Arrays.asList(
                e -> {
                    LOGGER.info("Received: {}", e);
                    assertThat(e).isInstanceOf(SubscriptionCreated.class);
                    final String subscriptionId = ((SubscriptionCreated) e).getSubscriptionId();
                    sendSignal(connectionName, RequestFromSubscription.of(subscriptionId, -1L, dittoHeaders));
                },
                e -> {
                    LOGGER.info("Received: {}", e);
                    assertThat(e).isInstanceOf(SubscriptionFailed.class);
                }));
    }

    @Test
    @Connections(NONE)
    public void testConnectionWithConfiguredIssuedAcknowledgement() {
        final String connectionName = "testConnectionWithConfiguredIssuedAcknowledgement";
        final Connection connection = cf.getSingleConnection(connectionName);
        final Connection connectionWithIssuedAcknowledgement = connection.toBuilder()
                .setTargets(connection.getTargets().stream()
                        .map(target -> ConnectivityModelFactory.newTargetBuilder(target)
                                .issuedAcknowledgementLabel(AcknowledgementLabel.of(connectionName + ":test"))
                                .build())
                        .collect(Collectors.toList()))
                .build();
        connectionsClient().testConnection(connectionWithIssuedAcknowledgement.toJson())
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    @Connections({CONNECTION1, CONNECTION2})
    public void sendSingleCommandAndEnsureEventIsProduced() {
        sendSingleCommandAndEnsureEventIsProduced(cf.connectionName1, cf.connectionName2);
    }

    @Test
    @Connections({CONNECTION1, CONNECTION2})
    public void sendMultipleCommandsAndEnsureEventsAreProduced() {
        // Given
        final String correlationId = createNewCorrelationId();
        final ThingId thingId = generateThingId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
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
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), createDittoHeaders(correlationId));

        final JsonPointer modAttrPointer = JsonPointer.of("modAttr");
        final JsonValue modAttrValue = JsonValue.of(42);

        final ModifyAttribute modifyAttribute =
                ModifyAttribute.of(thingId, modAttrPointer, modAttrValue, createDittoHeaders(correlationId));

        // When
        final C eventConsumer = initTargetsConsumer(cf.connectionName2);

        sendSignal(cf.connectionName1, createThing);

        consumeAndAssertEvents(cf.connectionName2, eventConsumer, Collections.singletonList(
                e -> {
                    final ThingCreated tc = thingEventForJson(e, ThingCreated.class, correlationId, thingId);
                    assertThat(tc.getRevision()).isEqualTo(1L);
                }), "ThingCreated");

        sendSignal(cf.connectionName1, modifyAttribute);
        sendSignal(cf.connectionName1, modifyAttribute);

        // Then wait for all events generated (order independent)
        consumeAndAssertEvents(cf.connectionName2, eventConsumer, Arrays.asList(
                e -> {
                    final AttributeCreated ac = thingEventForJson(e, AttributeCreated.class, correlationId, thingId);
                    assertThat(ac.getRevision()).isEqualTo(2L);
                    assertThat((Iterable<? extends JsonKey>) ac.getAttributePointer()).isEqualTo(modAttrPointer);
                    assertThat(ac.getAttributeValue()).isEqualTo(modAttrValue);
                },
                e -> {
                    final AttributeModified am = thingEventForJson(e, AttributeModified.class, correlationId, thingId);
                    assertThat(am.getRevision()).isEqualTo(3L);
                    assertThat((Iterable<? extends JsonKey>) am.getAttributePointer()).isEqualTo(modAttrPointer);
                    assertThat(am.getAttributeValue()).isEqualTo(modAttrValue);
                }
        ));
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1})
    public void sendHttpMessageAndProcessResponse() {
        // Given
        final ThingId thingId = generateThingId();
        final String featureId = "lamp";

        createNewThingWithPolicy(thingId, featureId, cf.connectionName1);

        final String requestMsgTopic = "toggle";
        final String requestMsgContentType = "text/plain";
        final String requestMsgBody = "toggle it, baby";

        final String expectedResponseContentType = "application/json";
        final String expectedResponseMsgBody = "{\"action\":\"I toggled!\"}";
        final HttpStatus expectedResponseMsgStatus = HttpStatus.PARTIAL_CONTENT;

        final String messageUrl = messagesResourceForToFeatureV2(thingId, featureId, requestMsgTopic);
        final String msgCorrelationId = UUID.randomUUID().toString();

        // check preservation of custom headers and blocking of sensitive headers
        final String customHeaderName = "Send-Http-Message-And-Process-Response-Header-Name";
        final String customHeaderValue = "senD-httP-messagE-anD-procesS-responsE-headeR-valuE";

        // When
        final C messageConsumer = initTargetsConsumer(cf.connectionName1);
        final CompletableFuture<Void> future = consumeFromTargetInFuture(cf.connectionName1, messageConsumer)
                .thenAccept(message -> {
                    if (message == null) {
                        final String detailMsg = "Message was not received in expected time!";
                        LOGGER.error(detailMsg);
                        throw new AssertionError(detailMsg);
                    }
                    try {
                        final Signal<?> signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(message));
                        assertThat(signal).isInstanceOf(MessageCommand.class);
                        assertThat(signal).isInstanceOf(SendFeatureMessage.class);
                        final SendFeatureMessage<?> messageCommand = (SendFeatureMessage<?>) signal;
                        assertThat(messageCommand.getFeatureId()).isEqualTo(featureId);

                        final Message<?> receivedMsg = messageCommand.getMessage();
                        assertThat(receivedMsg.getSubject()).isEqualTo(requestMsgTopic);
                        assertThat(receivedMsg.getPayload()).containsInstanceOf(String.class);
                        assertThat(receivedMsg.getPayload().map(o -> (String) o)).contains(requestMsgBody);

                        sendBackFeatureMessageResponse(thingId, featureId, receivedMsg.getHeaders().getSubject(),
                                messageCommand.getDittoHeaders().getCorrelationId().orElseThrow(),
                                expectedResponseContentType, expectedResponseMsgBody, expectedResponseMsgStatus);

                    } catch (final Throwable e) {
                        LOGGER.error("Got unexpected Throwable {}: {}", e.getClass().getName(), e.getMessage(), e);
                    }
                });

        // Then
        expect().statusCode(is(expectedResponseMsgStatus.getCode()))
                .contentType(is(expectedResponseContentType))
                .body(is(expectedResponseMsgBody))
                .given()
                .auth().oauth2(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .header(HttpHeader.X_CORRELATION_ID.getName(), msgCorrelationId)
                .header(HttpHeader.CONTENT_TYPE.getName(), requestMsgContentType)
                .header(customHeaderName, customHeaderValue)
                .body(requestMsgBody)
                .when()
                .post(messageUrl);
        // no point resending the message on error because the message consumer dies after first error.

        future.join();
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1})
    public void sendHttpMessageAndCheckParameterOrderHeader() {
        // Given
        final String msgCorrelationId = UUID.randomUUID().toString();
        final C messageConsumer = initTargetsConsumer(cf.connectionName1);

        final ThingId thingId = generateThingId();
        final String featureId = "lamp";

        createNewThingWithPolicy(thingId, featureId, cf.connectionName1);
        // There won't be a ThingCreated event because the command is sent by the same connection.
        // This test does not run for target-only connections.

        final String requestMsgTopic = "switchColor";
        final String requestAndResponseContentType = "application/json";
        final JsonObject requestMsgBody = JsonObject.newBuilder()
                .set("red", 100)
                .set("green", 200)
                .set("blue", 255)
                .build();

        final String messageUrl = messagesResourceForToFeatureV2(thingId, featureId, requestMsgTopic);
        final String orderedParameters = "[\"red\",\"green\",\"blue\"]";

        // When
        final CompletableFuture<Void> future = consumeFromTargetInFuture(cf.connectionName1, messageConsumer)
                .thenAccept(message -> {
                    if (message == null) {
                        final String detailMsg = "Message was not received in expected time!";
                        LOGGER.error(detailMsg);
                        throw new AssertionError(detailMsg);
                    }
                    try {
                        final Signal<?> signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(message));
                        assertThat(signal)
                                .describedAs("Expect MessageCommand; ThingCreated-event should be skipped " +
                                        "since it originates from the same connection.")
                                .isInstanceOf(MessageCommand.class);
                        assertThat(signal).isInstanceOf(SendFeatureMessage.class);
                        final SendFeatureMessage<?> messageCommand = (SendFeatureMessage<?>) signal;
                        assertThat(messageCommand.getFeatureId()).isEqualTo(featureId);

                        final Message<?> receivedMsg = messageCommand.getMessage();
                        assertThat(receivedMsg.getSubject()).isEqualTo(requestMsgTopic);
                        assertThat(receivedMsg.getPayload()).containsInstanceOf(JsonObject.class);
                        assertThat(receivedMsg.getPayload().map(o -> (JsonObject) o)).contains(requestMsgBody);
                        assertThat(receivedMsg.getHeaders().get(HttpHeader.X_THINGS_PARAMETER_ORDER.getName()))
                                .isEqualTo(orderedParameters);

                    } catch (final Throwable e) {
                        LOGGER.error("Got unexpected Throwable {}: {}", e.getClass().getName(), e.getMessage(), e);
                    }
                });
        // Then
        expect().statusCode(is(HttpStatus.ACCEPTED.getCode()))
                .given()
                .auth().oauth2(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .header(HttpHeader.X_CORRELATION_ID.getName(), msgCorrelationId)
                .header(HttpHeader.CONTENT_TYPE.getName(), requestAndResponseContentType)
                .body(requestMsgBody.toString())
                .queryParam("timeout", "0")
                .when()
                .post(messageUrl);
        // no point resending the message on error because the message consumer dies after first error.

        future.join();
    }

    private void sendBackFeatureMessageResponse(final ThingId thingId,
            final String featureId,
            final String subject,
            final String correlationId,
            final String contentType,
            final String expectedResponseMsgBody,
            final HttpStatus expectedResponseMsgStatus) {

        final MessageHeaders responseHeaders = MessageHeaders.newBuilder(MessageDirection.FROM, thingId, subject)
                .contentType(contentType)
                .correlationId(correlationId)
                .featureId(featureId)
                .build();

        final Message<String> responseMsg = Message.<String>newBuilder(responseHeaders)
                .payload(expectedResponseMsgBody)
                .build();
        final var messageResponse =
                SendFeatureMessageResponse.of(thingId, featureId, responseMsg, expectedResponseMsgStatus,
                        responseHeaders);

        sendSignal(cf.connectionName1, messageResponse);
    }

    @Test
    @Connections({CONNECTION1, CONNECTION_WITH_NAMESPACE_AND_RQL_FILTER})
    public void sendMultipleCommandsConsumeEventsFilteredByRql() {

        // Given
        final ThingBuilder.FromScratch thingBuilder = Thing.newBuilder();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(cf.connectionName1))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .forLabel("RESTRICTED")
                .setSubject(connectionSubject(cf.connectionNameWithNamespaceAndRqlFilter))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ)
                .build();

        final String correlationId = createNewCorrelationId();

        final ThingId thingId1 = generateThingId();
        final CreateThing createThing1 = CreateThing.of(thingBuilder.setId(thingId1).build(), policy.toJson(),
                createDittoHeaders(correlationId));

        final ThingId thingId2 = generateThingId(randomNamespace2);
        final CreateThing createThing2 = CreateThing.of(thingBuilder.setId(thingId2).build(), policy.toJson(),
                createDittoHeaders(correlationId));

        final ThingId thingId3 = generateThingId();
        final CreateThing createThing3 = CreateThing.of(thingBuilder.setId(thingId3).build(), policy.toJson(),
                createDittoHeaders(correlationId));

        final JsonPointer modAttrPointer = JsonPointer.of("counter");

        final int val1 = 23;
        final int val2 = 100;
        final int val3 = 50;
        final ModifyAttribute createAttribute1 = ModifyAttribute.of(thingId1, modAttrPointer, JsonValue.of(val1),
                createDittoHeaders(correlationId));
        final ModifyAttribute createAttribute2 = ModifyAttribute.of(thingId2, modAttrPointer, JsonValue.of(val2),
                createDittoHeaders(correlationId));
        final ModifyAttribute createAttribute3 = ModifyAttribute.of(thingId3, modAttrPointer, JsonValue.of(val3),
                createDittoHeaders(correlationId));

        final ModifyAttribute modifyAttribute1 = ModifyAttribute.of(thingId1, modAttrPointer, JsonValue.of(val1 * 2),
                createDittoHeaders(correlationId));
        final ModifyAttribute modifyAttribute2 = ModifyAttribute.of(thingId2, modAttrPointer, JsonValue.of(val2 * 2),
                createDittoHeaders(correlationId));
        final ModifyAttribute modifyAttribute3 = ModifyAttribute.of(thingId3, modAttrPointer, JsonValue.of(val3 * 2),
                createDittoHeaders(correlationId));

        // When
        final C eventConsumer = initTargetsConsumer(cf.connectionNameWithNamespaceAndRqlFilter);

        sendSignal(cf.connectionName1, createThing1);
        waitShort();
        sendSignal(cf.connectionName1, createThing2);
        waitShort();
        sendSignal(cf.connectionName1, createThing3);
        waitShort();
        sendSignal(cf.connectionName1, createAttribute1);
        waitShort();
        sendSignal(cf.connectionName1, createAttribute2);
        waitShort();
        sendSignal(cf.connectionName1, createAttribute3);
        waitShort();
        sendSignal(cf.connectionName1, modifyAttribute1);
        waitShort();
        sendSignal(cf.connectionName1, modifyAttribute2);
        waitShort();
        sendSignal(cf.connectionName1, modifyAttribute3);

        // Then wait for all events generated (order independent)
        // the used filter for twin events on connection5 is: ?namespaces=<DEFAULT_NAMESPACE_NAME>&filter=gt(attributes/counter,42)
        // the ThingCreated events must not be received - as the filter specifies that attributes/counter > 42
        consumeAndAssertEvents(cf.connectionNameWithNamespaceAndRqlFilter, eventConsumer, Arrays.asList(
                e -> {
                    final AttributeCreated ac = thingEventForJson(e, AttributeCreated.class, correlationId, thingId3);
                    assertThat(ac.getRevision()).isEqualTo(2L);
                    assertThat((Iterable<? extends JsonKey>) ac.getAttributePointer()).isEqualTo(modAttrPointer);
                    assertThat(ac.getAttributeValue()).isEqualTo(JsonValue.of(val3));
                },
                e -> {
                    final AttributeModified am = thingEventForJson(e, AttributeModified.class, correlationId, thingId1);
                    assertThat(am.getRevision()).isEqualTo(3L);
                    assertThat((Iterable<? extends JsonKey>) am.getAttributePointer()).isEqualTo(modAttrPointer);
                    assertThat(am.getAttributeValue()).isEqualTo(JsonValue.of(val1 * 2));
                },
                e -> {
                    final AttributeModified am = thingEventForJson(e, AttributeModified.class, correlationId, thingId3);
                    assertThat(am.getRevision()).isEqualTo(3L);
                    assertThat((Iterable<? extends JsonKey>) am.getAttributePointer()).isEqualTo(modAttrPointer);
                    assertThat(am.getAttributeValue()).isEqualTo(JsonValue.of(val3 * 2));
                }
        ));
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION_WITH_ENFORCEMENT_ENABLED})
    public void sendMessageAndExpectRejectedBecauseEnforcementFailed() {
        final ThingId thingId = generateThingId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();

        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(connectionSubject(cf.connectionNameWithEnforcementEnabled))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();

        sendCreateThingAndExpectError(thing, policy, cf.connectionNameWithEnforcementEnabled, HttpStatus.BAD_REQUEST,
                entry(DEVICE_ID_HEADER, "wrong:thingId"));
    }

    @Test
    @Category(RequireSource.class)
    @Connections(CONNECTION1)
    public void sendMessageWithTimeoutAndExpectErrorResponse() {

        final ThingId thingId = sendCreateThingAndEnsureResponseIsSentBack(cf.connectionName1);

        final String correlationId = name.getMethodName() + "-" + Long.toHexString(System.currentTimeMillis());
        final DittoHeaders headers = DittoHeaders.newBuilder()
                .correlationId(correlationId)
                .timeout(Duration.ofSeconds(2))
                .build();
        final Message<?> message = Message.newBuilder(
                MessageBuilder.newHeadersBuilder(MessageDirection.TO, thingId, "timeout")
                        .contentType("text/plain")
                        .build())
                .payload("no one will ever respond :/")
                .build();
        final SendThingMessage<?> sendThingMessage = SendThingMessage.of(thingId, message, headers);
        final ErrorResponse<?> response =
                sendSignalAndExpectError(sendThingMessage, cf.connectionName1, HttpStatus.REQUEST_TIMEOUT,
                        correlationId);
        final DittoRuntimeException exception = response.getDittoRuntimeException();
        assertThat(exception).isInstanceOf(CommandTimeoutException.class);
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION_WITH_ENFORCEMENT_ENABLED})
    public void createPolicyAndExpectRejectedBecauseEnforcementFailed() {
        final PolicyId policyId = generatePolicyId();

        final Policy policy = createNewPolicy(policyId, "notused", cf.connectionNameWithEnforcementEnabled);

        final String correlationId = UUID.randomUUID().toString();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(correlationId)
                .putHeader(DEVICE_ID_HEADER, "wrong:policyId")
                .build();

        final CreatePolicy createPolicy = CreatePolicy.of(policy, dittoHeaders);

        final Jsonifiable<JsonObject> response =
                sendSignalAndExpectError(createPolicy, cf.connectionNameWithEnforcementEnabled, HttpStatus.BAD_REQUEST,
                        correlationId);

        assertThat(response).isInstanceOf(PolicyErrorResponse.class);
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION_WITH_ENFORCEMENT_ENABLED})
    public void sendMessageAndExpectMessageIsProcessedBecauseEnforcementSucceeded() {
        sendCreateThingAndEnsureResponseIsSentBack(cf.connectionNameWithEnforcementEnabled);
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION_WITH_ENFORCEMENT_ENABLED})
    public void createPolicyAndExpectMessageIsProcessedBecauseEnforcementSucceeded() {
        final PolicyId policyId = generatePolicyId();
        final Policy policy = createNewPolicy(policyId, "notused", cf.connectionNameWithEnforcementEnabled);

        final String correlationId = UUID.randomUUID().toString();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(correlationId)
                .putHeader(DEVICE_ID_HEADER, policyId)
                .build();

        final CreatePolicy createPolicy = CreatePolicy.of(policy, dittoHeaders);

        final CommandResponse<?> commandResponse =
                sendCommandAndEnsureResponseIsSentBack(cf.connectionNameWithEnforcementEnabled, createPolicy);
        assertThat(commandResponse).isInstanceOf(CreatePolicyResponse.class);
        assertThat(commandResponse.getHttpStatus()).isEqualTo(HttpStatus.CREATED);
        assertThat(((CreatePolicyResponse) commandResponse).getPolicyCreated()
                .flatMap(Policy::getEntityId))
                .contains(policyId);
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1})
    public void createThingWithMultipleSlashesInFeatureProperty() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final TopicPath topicPath = TopicPath.newBuilder(thingId).things().twin().commands().modify().build();
        final String correlationId = UUID.randomUUID().toString();
        final String createFeatureWithMultipleSlashesInPath = "{" +
                "\"topic\": \"" + topicPath.getPath() + "\", " +
                "\"path\": \"/features/feature/properties/slash//es\", " +
                "\"headers\":{\"content-type\":\"application/vnd.eclipse.ditto+json\",\"version\":1,\"correlation-id\":\"" +
                correlationId + "\"}," +
                "\"payload\": 13" +
                "}";

        // When
        final C consumer = initResponseConsumer(cf.connectionName1, correlationId);
        sendAsJsonString(cf.connectionName1, correlationId, createFeatureWithMultipleSlashesInPath,
                DittoHeaders.newBuilder().correlationId(correlationId).build());

        // Then
        final M message = consumeResponse(correlationId, consumer);
        assertThat(message).isNotNull();
        assertThat(getCorrelationId(message)).isEqualTo(correlationId);

        final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(message);
        final Adaptable eventAdaptable1 = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
        final Jsonifiable<JsonObject> event = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable1);
        assertThat(event).isInstanceOf(ThingErrorResponse.class);
        assertThat(((ThingErrorResponse) event).getDittoRuntimeException().getDescription())
                .contains("Consecutive slashes in JSON pointers are not supported.");

    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1})
    public void sendAndReceiveSearchProtocolMessages() {
        testSearchProtocol(cf.connectionName1);
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1})
    public void sendAndReceiveStreamingProtocolMessagesForPersistedEvents() {
        final String connectionName = cf.connectionName1;
        LOGGER.info("Testing Streaming Subscription with connection: {}", connectionName);

        // Given
        final ThingId thingId = generateThingId();
        final String featureId = "lamp";

        final Policy policy = createNewPolicy(thingId, featureId, connectionName);
        putPolicy(policy)
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .withCorrelationId(UUID.randomUUID().toString())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(policy.getEntityId().orElseThrow())
                .setFeature(featureId, FeatureProperties.newBuilder().build())
                .build();
        putThing(2, thing, JsonSchemaVersion.V_2)
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .withCorrelationId(UUID.randomUUID().toString())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final String correlationId = UUID.randomUUID().toString();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(correlationId)
                .putHeader(DEVICE_ID_HEADER, thingId)
                .build();
        final SubscribeForPersistedEvents createSubscription =
                SubscribeForPersistedEvents.of(thingId, JsonPointer.empty(), null, null, dittoHeaders);

        final C eventConsumer = initResponseConsumer(connectionName, correlationId);
        sendSignal(connectionName, createSubscription);
        consumeAndAssertEvents(connectionName, eventConsumer, Arrays.asList(
                e -> {
                    LOGGER.info("Received: {}", e);
                    assertThat(e).isInstanceOf(StreamingSubscriptionCreated.class);
                    final String subscriptionId = ((StreamingSubscriptionCreated) e).getSubscriptionId();
                    sendSignal(connectionName,
                            RequestFromStreamingSubscription.of(thingId, JsonPointer.empty(), subscriptionId, 1L, dittoHeaders));
                },
                e -> {
                    LOGGER.info("Received: {}", e);
                    assertThat(e).isInstanceOf(StreamingSubscriptionHasNext.class);
                    final StreamingSubscriptionHasNext subscriptionHasNext = (StreamingSubscriptionHasNext) e;
                    final JsonValue item = subscriptionHasNext.getItem();
                    assertThat(item.isObject()).isTrue();

                    final Signal<?> historicalSignal = DittoProtocolAdapter.newInstance().fromAdaptable(
                            ProtocolFactory.jsonifiableAdaptableFromJson(item.asObject()));

                    assertThat(historicalSignal).isInstanceOf(ThingEvent.class);
                    final ThingEvent<?> thingEvent = (ThingEvent<?>) historicalSignal;
                    if (thingEvent.getRevision() == 1L) {
                        assertThat(thingEvent).isInstanceOf(ThingCreated.class);
                    } else {
                        fail("Only expected the creation event!");
                    }
                },
                e -> {
                    LOGGER.info("Received: {}", e);
                    assertThat(e).isInstanceOf(StreamingSubscriptionComplete.class);
                }));
    }

    private void testSearchProtocol(final String connectionName) {

        LOGGER.info("Testing Subscription with connection: {}", connectionName);
        final String correlationId = UUID.randomUUID().toString();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(correlationId)
                .putHeader(DEVICE_ID_HEADER, ThingId.of("test:search-protocol-thingId"))
                .build();
        final CreateSubscription createSubscription = CreateSubscription.of(dittoHeaders);

        testSearchProtocol(connectionName, createSubscription);
    }

    @Test
    @Connections({CONNECTION_WITH_EXTRA_FIELDS, CONNECTION1})
    public void publishEnrichedSignals() {
        final String correlationIdPrefix = "publishEnrichedSignals-";
        final String correlationId = correlationIdPrefix + createNewCorrelationId();
        final DittoHeaders headers = createDittoHeaders(correlationId);

        final C consumer = initTargetsConsumer(cf.connectionNameWithExtraFields);

        // Given
        final ThingId thingId = generateThingId();
        final int initialCounterValue = 0;
        final String counterKey = "counter";
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setAttribute(JsonPointer.of(counterKey), JsonValue.of(initialCounterValue))
                .build();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(cf.connectionName1))
                .setSubject(connectionSubject(cf.connectionNameWithExtraFields))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), headers);

        // Twin event (thingCreated) - received
        sendSignal(cf.connectionName1, createThing);
        final M thingCreatedMessage = consumeFromTarget(cf.connectionNameWithExtraFields, consumer);
        assertThat(thingCreatedMessage).describedAs("twinEvent (thingCreated)").isNotNull();
        final Adaptable thingCreatedAdaptable = jsonifiableAdaptableFrom(thingCreatedMessage);
        assertThat(thingCreatedAdaptable.getTopicPath().getChannel()).describedAs("thingCreated/channel")
                .isEqualTo(TopicPath.Channel.TWIN);
        assertThat(thingCreatedAdaptable.getTopicPath().getAction()).describedAs("thingCreated/action")
                .contains(TopicPath.Action.CREATED);
        assertThat(thingCreatedAdaptable.getPayload().getExtra()).describedAs("thingCreated/extra")
                .contains(JsonObject.newBuilder()
                        .set(JsonPointer.of("attributes/" + counterKey), initialCounterValue)
                        .build()
                );


        final ModifyAttribute modifyAttribute = ModifyAttribute.of(thingId, JsonPointer.of(counterKey),
                JsonValue.of(42), headers.toBuilder()
                        .correlationId(correlationIdPrefix + "twin-modify-attr-" + createNewCorrelationId())
                        .build());
        // Twin event (attributeModified) - received
        //  as this command changed the same "counter" which was subscribed "extraFields", the new value of the counter
        //  must take priority
        sendSignal(cf.connectionName1, modifyAttribute);
        final M thingAttributeModifiedMessage = consumeFromTarget(cf.connectionNameWithExtraFields, consumer);
        assertThat(thingAttributeModifiedMessage).describedAs("twinEvent (attributeModified)").isNotNull();
        final Adaptable thingAttributeModifiedAdaptable = jsonifiableAdaptableFrom(thingAttributeModifiedMessage);
        assertThat(thingAttributeModifiedAdaptable.getTopicPath().getChannel()).describedAs(
                "attributeModified[c=42]/channel")
                .isEqualTo(TopicPath.Channel.TWIN);
        assertThat(thingAttributeModifiedAdaptable.getTopicPath().getAction()).describedAs(
                "attributeModified[c=42]/action")
                .contains(TopicPath.Action.MODIFIED);
        assertThat(thingAttributeModifiedAdaptable.getPayload().getExtra()).describedAs("attributeModified[c=42]/extra")
                .contains(JsonObject.newBuilder()
                        .set(JsonPointer.of("attributes/" + counterKey), 42)
                        .build()
                );

        final MergeThing mergeThing = MergeThing.of(thingId, JsonPointer.of("/attributes/" + counterKey),
                JsonValue.of(1337), headers.toBuilder()
                        .correlationId(correlationIdPrefix + "twin-merge-th-" + createNewCorrelationId())
                        .build());
        // Twin event (thingMerged) - received
        //  as this command changed the same "counter" which was subscribed "extraFields", the new value of the counter
        //  must take priority
        sendSignal(cf.connectionName1, mergeThing);
        final M thingMergedMessage = consumeFromTarget(cf.connectionNameWithExtraFields, consumer);
        assertThat(thingMergedMessage).describedAs("twinEvent (thingMerged)").isNotNull();
        final Adaptable thingMergedAdaptable = jsonifiableAdaptableFrom(thingMergedMessage);
        assertThat(thingMergedAdaptable.getTopicPath().getChannel()).describedAs(
                "thingMerged[c=1337]/channel")
                .isEqualTo(TopicPath.Channel.TWIN);
        assertThat(thingMergedAdaptable.getTopicPath().getAction()).describedAs(
                "thingMerged[c=42]/action")
                .contains(TopicPath.Action.MERGED);
        assertThat(thingMergedAdaptable.getPayload().getExtra()).describedAs("thingMerged[c=1337]/extra")
                .contains(JsonObject.newBuilder()
                        .set(JsonPointer.of("attributes/" + counterKey), 1337)
                        .build()
                );

        // Live command - not received
        final DittoHeaders liveHeaders = headers.toBuilder()
                .correlationId(correlationIdPrefix + "live-" + createNewCorrelationId())
                .responseRequired(false)
                .channel(TopicPath.Channel.LIVE.getName()).build();
        final ModifyAttribute liveModifyAttribute =
                ModifyAttribute.of(thingId, JsonPointer.of(counterKey), JsonValue.of(99), liveHeaders);
        sendSignal(cf.connectionName1, liveModifyAttribute);

        // Live event - not received
        final DittoHeaders liveHeaders2 = liveHeaders.toBuilder()
                .correlationId(correlationIdPrefix + "live-" + createNewCorrelationId())
                .build();
        final AttributeModified liveEvent =
                AttributeModified.of(thingId, JsonPointer.of("filter"), JsonValue.of(true), 100L, Instant.now(),
                        liveHeaders2, null);
        sendSignal(cf.connectionName1, liveEvent);

        // Twin event (attributeModified[c=99]) - received
        // When this event is received, all previous live events have passed through the connection's outgoing stream.
        // It is then safe to update the counter attribute to enable receiving of live signals.
        final DittoHeaders headers2 = headers.toBuilder()
                .correlationId(correlationIdPrefix + createNewCorrelationId())
                .build();
        final ModifyAttribute setCounter99 =
                ModifyAttribute.of(thingId, JsonPointer.of(counterKey), JsonValue.of(99), headers2);
        sendSignal(cf.connectionName1, setCounter99);
        final M setCounter99Message = consumeFromTarget(cf.connectionNameWithExtraFields, consumer);
        assertThat(setCounter99Message).describedAs("twinEvent (attributeModified[c=99])").isNotNull();
        final Adaptable setCounter99Adaptable = jsonifiableAdaptableFrom(setCounter99Message);
        assertThat(setCounter99Adaptable.getPayload().getExtra()).describedAs("attributeModified[c=99]/extra")
                .contains(JsonObject.newBuilder().set(JsonPointer.of("attributes/counter"), 99).build());

        // Twin event (attributeModified[c=20]) - received
        final DittoHeaders headers3 = headers.toBuilder()
                .correlationId(correlationIdPrefix + createNewCorrelationId())
                .build();
        final ModifyAttribute setCounter20 =
                ModifyAttribute.of(thingId, JsonPointer.of(counterKey), JsonValue.of(20), headers3);
        sendSignal(cf.connectionName1, setCounter20);
        final M setCounter20Message = consumeFromTarget(cf.connectionNameWithExtraFields, consumer);
        assertThat(setCounter20Message).describedAs("twinEvent (attributeModified[c=20])").isNotNull();
        final Adaptable setCounter20Adaptable = jsonifiableAdaptableFrom(setCounter20Message);
        assertThat(setCounter20Adaptable.getTopicPath().getChannel()).describedAs("attributeModified[c=20]/channel")
                .isEqualTo(TopicPath.Channel.TWIN);
        assertThat(setCounter20Adaptable.getTopicPath().getAction()).describedAs("attributeModified[c=20]/action")
                .contains(TopicPath.Action.MODIFIED);
        assertThat(setCounter20Adaptable.getPayload().getPath().toString()).describedAs("attributeModified[c=20]/path")
                .isEqualTo(JsonPointer.of("attributes/counter").toString());
        assertThat(setCounter20Adaptable.getPayload().getExtra()).describedAs("attributeModified[c=20]/extra")
                .contains(JsonObject.newBuilder().set(JsonPointer.of("attributes/counter"), 20).build());

        // Live event - not received due to filter
        final DittoHeaders liveHeaders3 = liveHeaders.toBuilder()
                .correlationId(correlationIdPrefix + "live-" + createNewCorrelationId())
                .build();
        final AttributeModified liveEventFilterFalse =
                AttributeModified.of(thingId, JsonPointer.of("filter"), JsonValue.of(false), 200L, Instant.now(),
                        liveHeaders3, null);
        sendSignal(cf.connectionName1, liveEventFilterFalse);

        // Live command - received
        final DittoHeaders liveHeaders4 = liveHeaders.toBuilder()
                .correlationId(correlationIdPrefix + "live-" + createNewCorrelationId())
                .build();
        sendSignal(cf.connectionName1, liveModifyAttribute.setDittoHeaders(liveHeaders4));
        final M liveCommandMessage = consumeFromTarget(cf.connectionNameWithExtraFields, consumer);
        assertThat(liveCommandMessage).describedAs("liveCommand").isNotNull();
        final Adaptable liveCommandAdaptable = jsonifiableAdaptableFrom(liveCommandMessage);
        assertThat(liveCommandAdaptable.getTopicPath().getChannel()).describedAs("liveCommand/channel")
                .isEqualTo(TopicPath.Channel.LIVE);
        assertThat(liveCommandAdaptable.getTopicPath().getAction()).describedAs("liveCommand/action")
                .contains(TopicPath.Action.MODIFY);
        assertThat(liveCommandAdaptable.getPayload().getPath().toString()).describedAs("liveCommand/path")
                .isEqualTo(JsonPointer.of("attributes/counter").toString());
        assertThat(liveCommandAdaptable.getPayload().getValue().map(JsonValue::asInt)).describedAs("liveCommand/value")
                .contains(99);
        assertThat(liveCommandAdaptable.getPayload().getExtra()).describedAs("liveCommand/extra")
                .contains(JsonObject.newBuilder().set(JsonPointer.of("attributes/counter"), 20).build());

        // Live event - received
        final DittoHeaders liveHeaders5 = liveHeaders.toBuilder()
                .correlationId(correlationIdPrefix + "live-" + createNewCorrelationId())
                .build();
        final AttributeModified liveEventFilterTrue = liveEvent.setDittoHeaders(liveHeaders5).setRevision(300L);
        sendSignal(cf.connectionName1, liveEventFilterTrue);
        final M liveEventMessage = consumeFromTarget(cf.connectionNameWithExtraFields, consumer);
        assertThat(liveEventMessage).describedAs("twinEvent (attributeModified)").isNotNull();
        final Adaptable liveEventAdaptable = jsonifiableAdaptableFrom(liveEventMessage);
        assertThat(liveEventAdaptable.getTopicPath().getChannel()).describedAs("attributeModified/channel")
                .isEqualTo(TopicPath.Channel.LIVE);
        assertThat(liveEventAdaptable.getTopicPath().getAction()).describedAs("attributeModified/action")
                .contains(TopicPath.Action.MODIFIED);
        assertThat(liveEventAdaptable.getPayload().getPath().toString()).describedAs("attributeModified/path")
                .isEqualTo(JsonPointer.of("attributes/filter").toString());
        assertThat(liveEventAdaptable.getPayload().getExtra()).describedAs("attributeModified/extra")
                .contains(JsonObject.newBuilder().set(JsonPointer.of("attributes/counter"), 20).build());

        final DittoHeaders deletionHeaders = liveHeaders.toBuilder()
                .correlationId(correlationIdPrefix + "delete-" + createNewCorrelationId())
                .build();
        final DeleteThing deleteThing = DeleteThing.of(thingId, deletionHeaders);
        sendSignal(cf.connectionName1, deleteThing);
        final M thingDeleted = consumeFromTarget(cf.connectionNameWithExtraFields, consumer);
        assertThat(thingDeleted).describedAs("twinEvent (thingDeleted)").isNotNull();
        final Adaptable thingDeletedAdaptable = jsonifiableAdaptableFrom(thingDeleted);
        assertThat(thingDeletedAdaptable.getPayload().getExtra()).describedAs("thingDeleted/extra")
                .contains(JsonObject.newBuilder().set(JsonPointer.of("attributes/counter"), 20).build());
    }

    @Test
    @Connections({CONNECTION_WITH_EXTRA_FIELDS, CONNECTION1})
    public void publishEnrichedSignalsWithFeatureWildcard() {
        final String correlationIdPrefix = "publishEnrichedSignalsWithWildcard-";
        final String correlationId = correlationIdPrefix + createNewCorrelationId();
        final DittoHeaders headers = createDittoHeaders(correlationId);

        final C consumer = initTargetsConsumer(cf.connectionNameWithExtraFields);

        // Given
        final ThingId thingId = generateThingId();
        final int initialCounterValue = 0;
        final String counterKey = "counter";
        final FeatureProperties featureProperties =
                ThingsModelFactory.newFeaturePropertiesBuilder()
                        .set(counterKey, initialCounterValue)
                        .set("connected", true)
                        .build();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setFeature("f1", featureProperties)
                .setFeature("f2", featureProperties)
                .setFeature("f3", ThingsModelFactory.newFeaturePropertiesBuilder().set("connected", false).build())
                .build();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(cf.connectionName1))
                .setSubject(connectionSubject(cf.connectionNameWithExtraFields))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), headers);

        // Twin event (thingCreated) - received
        sendSignal(cf.connectionName1, createThing);
        final M thingCreatedMessage = consumeFromTarget(cf.connectionNameWithExtraFields, consumer);
        assertThat(thingCreatedMessage).describedAs("twinEvent (thingCreated)").isNotNull();
        final Adaptable thingCreatedAdaptable = jsonifiableAdaptableFrom(thingCreatedMessage);
        assertThat(thingCreatedAdaptable.getTopicPath().getChannel()).describedAs("thingCreated/channel")
                .isEqualTo(TopicPath.Channel.TWIN);
        assertThat(thingCreatedAdaptable.getTopicPath().getAction()).describedAs("thingCreated/action")
                .contains(TopicPath.Action.CREATED);
        assertThat(thingCreatedAdaptable.getPayload().getExtra()).describedAs("thingCreated/extra")
                .contains(JsonObject.newBuilder()
                        .set(JsonPointer.of("features/f1/properties/" + counterKey), initialCounterValue)
                        .set(JsonPointer.of("features/f2/properties/" + counterKey), initialCounterValue)
                        .set(JsonPointer.of("features/f1/properties/connected"), true)
                        .set(JsonPointer.of("features/f2/properties/connected"), true)
                        .set(JsonPointer.of("features/f3/properties/connected"), false)
                        .build()
                );

        final ModifyFeatureProperty modifyProperty = ModifyFeatureProperty.of(thingId, "f1",
                JsonPointer.of(counterKey), JsonValue.of(42), headers.toBuilder()
                        .correlationId(correlationIdPrefix + "twin-modify-prop-" + createNewCorrelationId())
                        .build());

        // Twin event (featurePropertyModified) - received
        //  as this command changed the same "counter" which was subscribed "extraFields", the new value of the counter
        //  must take priority
        sendSignal(cf.connectionName1, modifyProperty);
        final M thingPropertyModifiedMessage = consumeFromTarget(cf.connectionNameWithExtraFields, consumer);
        assertThat(thingPropertyModifiedMessage).describedAs("twinEvent (featurePropertyModified)").isNotNull();
        final Adaptable thingPropertyModifiedAdaptable = jsonifiableAdaptableFrom(thingPropertyModifiedMessage);
        assertThat(thingPropertyModifiedAdaptable.getTopicPath().getChannel()).describedAs(
                        "featurePropertyModified[c=42]/channel")
                .isEqualTo(TopicPath.Channel.TWIN);
        assertThat(thingPropertyModifiedAdaptable.getTopicPath().getAction()).describedAs(
                        "featurePropertyModified[c=42]/action")
                .contains(TopicPath.Action.MODIFIED);
        assertThat(thingPropertyModifiedAdaptable.getPayload().getExtra()).describedAs(
                        "featurePropertyModified[c=42]/extra")
                .contains(JsonObject.newBuilder()
                        .set(JsonPointer.of("features/f1/properties/" + counterKey), 42)
                        .set(JsonPointer.of("features/f2/properties/" + counterKey), initialCounterValue)
                        .set(JsonPointer.of("features/f1/properties/connected"), true)
                        .build()
                );

        final MergeThing mergeThing = MergeThing.of(thingId, JsonPointer.of("/features"),
                JsonObject.newBuilder()
                        .set(JsonPointer.of("f1/properties/" + counterKey), 1337)
                        .set(JsonPointer.of("f2/properties/" + counterKey), 42)
                        .build(), headers.toBuilder()
                        .correlationId(correlationIdPrefix + "twin-merge-th-" + createNewCorrelationId())
                        .build());

        // Twin event (thingMerged) - received
        //  as this command changed the same "counter" which was subscribed "extraFields", the new value of the counter
        //  must take priority
        sendSignal(cf.connectionName1, mergeThing);
        final M thingMergedMessage = consumeFromTarget(cf.connectionNameWithExtraFields, consumer);
        assertThat(thingMergedMessage).describedAs("twinEvent (thingMerged)").isNotNull();
        final Adaptable thingMergedAdaptable = jsonifiableAdaptableFrom(thingMergedMessage);
        assertThat(thingMergedAdaptable.getTopicPath().getChannel()).describedAs(
                        "thingMerged[c=1337]/channel")
                .isEqualTo(TopicPath.Channel.TWIN);
        assertThat(thingMergedAdaptable.getTopicPath().getAction()).describedAs(
                        "thingMerged[c=42]/action")
                .contains(TopicPath.Action.MERGED);
        assertThat(thingMergedAdaptable.getPayload().getExtra()).describedAs("thingMerged[c=1337]/extra")
                .contains(JsonObject.newBuilder()
                        .set(JsonPointer.of("features/f1/properties/" + counterKey), 1337)
                        .set(JsonPointer.of("features/f2/properties/" + counterKey), 42)
                        .set(JsonPointer.of("features/f1/properties/connected"), true)
                        .set(JsonPointer.of("features/f2/properties/connected"), true)
                        .build()
                );
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1, CONNECTION2})
    public void sendTwinAndLiveCommandsWithAcknowledgementRequests() {
        // GIVEN: Twin is created with policy granting access to connections 1 and 2
        final String correlationId = createNewCorrelationId();
        final ThingId thingId = generateThingId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(putPolicyForThing(thingId, cf.connectionName1, cf.connectionName2))
                .build();

        final AcknowledgementRequest twinPersistedAckRequest =
                AcknowledgementRequest.of(DittoAcknowledgementLabel.TWIN_PERSISTED);
        final AcknowledgementLabel customAck = connectionScopedAckLabel(cf.connectionName2, "custom", cf);
        final AcknowledgementRequest customAckRequest = AcknowledgementRequest.of(customAck);
        final CreateThing createThing = CreateThing.of(thing, null, createDittoHeaders(correlationId)
                .toBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .acknowledgementRequest(twinPersistedAckRequest, customAckRequest)
                .build());

        // WHEN: Twin create command with ack requests are sent
        final C responseConsumer = initResponseConsumer(cf.connectionName1, correlationId);
        final C eventConsumer = initTargetsConsumer(cf.connectionName2);

        sendSignal(cf.connectionName1, createThing);

        final JsonValue customAckPayload = JsonObject.newBuilder()
                .set("this_is_custom", "hooray")
                .build();
        consumeAndAssertEvents(cf.connectionName2, eventConsumer, Collections.singletonList(
                e -> {
                    final ThingCreated tc = thingEventForJson(e, ThingCreated.class, correlationId, thingId);
                    assertThat(tc.getRevision()).isEqualTo(1L);
                    assertThat(tc.getDittoHeaders().getAcknowledgementRequests()).contains(customAckRequest);
                    assertThat(tc.getDittoHeaders().getAcknowledgementRequests())
                            .doesNotContain(twinPersistedAckRequest);

                    sendSignal(cf.connectionName2, Acknowledgement.of(customAck, thingId, HttpStatus.ALREADY_REPORTED,
                            tc.getDittoHeaders(), customAckPayload));
                }));

        // THEN: response with aggregated acks is received
        final M response = consumeResponse(correlationId, responseConsumer);
        assertThat(response).isNotNull();
        assertThat(getCorrelationId(response)).isEqualTo(correlationId);

        final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(response);
        final Adaptable eventAdaptable1 = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
        final Jsonifiable<JsonObject> theResponse = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable1);
        assertThat(theResponse).isInstanceOf(Acknowledgements.class);

        final Acknowledgements acks = (Acknowledgements) theResponse;
        assertThat(acks.getHttpStatus()).isEqualTo(HttpStatus.OK);
        assertThat(acks.getSize()).isEqualTo(2);
        final Set<Acknowledgement> successfulAcknowledgements = acks.getSuccessfulAcknowledgements();
        assertThat(successfulAcknowledgements.stream()
                .map(Acknowledgement::getLabel)
        ).contains(twinPersistedAckRequest.getLabel(), customAckRequest.getLabel());

        assertThat(filterByLabel(successfulAcknowledgements, twinPersistedAckRequest.getLabel())
                .map(Acknowledgement::getHttpStatus)
                .findAny()
        ).contains(HttpStatus.CREATED);
        assertThat(filterByLabel(successfulAcknowledgements, twinPersistedAckRequest.getLabel())
                .map(Acknowledgement::getEntity)
                .flatMap(Optional::stream)
                .map(JsonValue::asObject)
                .map(ThingsModelFactory::newThing)
                .findAny()
        ).contains(thing);

        assertThat(filterByLabel(successfulAcknowledgements, customAckRequest.getLabel())
                .map(Acknowledgement::getHttpStatus)
                .findAny()
        ).contains(HttpStatus.ALREADY_REPORTED);
        assertThat(filterByLabel(successfulAcknowledgements, customAckRequest.getLabel())
                .map(Acknowledgement::getEntity)
                .flatMap(Optional::stream)
                .findAny()
        ).contains(customAckPayload);

        // WHEN: a live command is sent and answered by its requested ack
        final String liveCorrelationId = createNewCorrelationId();
        final C liveResponseConsumer = initResponseConsumer(cf.connectionName1, liveCorrelationId);
        sendSignal(cf.connectionName1, createThing.setDittoHeaders(createThing.getDittoHeaders()
                .toBuilder()
                .channel(TopicPath.Channel.LIVE.getName())
                .correlationId(liveCorrelationId)
                .acknowledgementRequests(Collections.singleton(customAckRequest))
                .build()
        ));

        consumeAndAssertEvents(cf.connectionName2, eventConsumer, Collections.singletonList(
                e -> {
                    assertThat(e).isInstanceOf(CreateThing.class);
                    final CreateThing ct = (CreateThing) e;
                    assertThat(ct.getDittoHeaders().getChannel()).contains(TopicPath.Channel.LIVE.getName());
                    assertThat(ct.getDittoHeaders().getAcknowledgementRequests())
                            .isEqualTo(Collections.singleton(customAckRequest));

                    sendSignal(cf.connectionName2, Acknowledgement.of(customAck, thingId, HttpStatus.ALREADY_REPORTED,
                            ct.getDittoHeaders(), customAckPayload));
                    sendSignal(cf.connectionName2, CreateThingResponse.of(ct.getThing(), ct.getDittoHeaders()));
                }));

        // THEN: the acknowledgement is received.
        final M liveResponse = consumeResponse(liveCorrelationId, liveResponseConsumer);
        assertThat(liveResponse).isNotNull();
        assertThat(getCorrelationId(liveResponse)).isEqualTo(liveCorrelationId);

        final Jsonifiable<JsonObject> theLiveResponse = PROTOCOL_ADAPTER.fromAdaptable(
                ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptableFrom(liveResponse)).build());
        assertThat(theLiveResponse).isInstanceOf(Acknowledgements.class);

        final Acknowledgements liveAcks = (Acknowledgements) theLiveResponse;
        assertThat(liveAcks.getSize()).isEqualTo(2);
        final Optional<Acknowledgement> customAcknowledgement = liveAcks.getAcknowledgement(customAck);
        assertThat(customAcknowledgement).isPresent();
        assertThat(customAcknowledgement.get().getHttpStatus()).isEqualTo(HttpStatus.ALREADY_REPORTED);
        final Optional<Acknowledgement> liveResponseAcknowledgement =
                liveAcks.getAcknowledgement(DittoAcknowledgementLabel.LIVE_RESPONSE);
        assertThat(liveResponseAcknowledgement).isPresent();
        assertThat(liveResponseAcknowledgement.get().getHttpStatus()).isEqualTo(HttpStatus.CREATED);
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1, CONNECTION2})
    public void sendModifyThingWithAcknowledgementRequestsForRedelivery() {
        // Given
        final String thingCreatedCorrelationId = createNewCorrelationId();
        final String correlationId = createNewCorrelationId();
        final C eventConsumer = initTargetsConsumer(cf.connectionName2);
        final ThingId thingId = generateThingId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(putPolicyForThing(thingId, cf.connectionName1, cf.connectionName2))
                .build();

        putThing(2, thing, JsonSchemaVersion.V_2)
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .withCorrelationId(thingCreatedCorrelationId)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final AcknowledgementRequest twinPersistedAckRequest =
                AcknowledgementRequest.of(DittoAcknowledgementLabel.TWIN_PERSISTED);
        final AcknowledgementLabel customAck = connectionScopedAckLabel(cf.connectionName2, "custom", cf);
        final AcknowledgementRequest customAckRequest = AcknowledgementRequest.of(customAck);
        final ModifyThing modifyThing = ModifyThing.of(thingId, thing, null, createDittoHeaders(correlationId)
                .toBuilder()
                .acknowledgementRequest(twinPersistedAckRequest, customAckRequest)
                .schemaVersion(JsonSchemaVersion.V_2)
                .build());

        // Given: ModifyThing with custom requested ack is sent
        final C responseConsumer = initResponseConsumer(cf.connectionName1, correlationId);
        sendSignal(cf.connectionName1, modifyThing);

        final AtomicLong thingCreatedRevision = new AtomicLong(Long.MIN_VALUE);
        consumeAndAssertEvents(cf.connectionName2, eventConsumer, List.of(
                created -> {
                    // Sanity check: ThingCreated has expected sequence number 1 (no accidental ID collision)
                    final ThingCreated thingCreated =
                            thingEventForJson(created, ThingCreated.class, thingCreatedCorrelationId, thingId);
                    thingCreatedRevision.set(thingCreated.getRevision());
                },
                // When: First ThingModified event is acknowledged negatively
                e -> {
                    final ThingModified thingModified =
                            thingEventForJson(e, ThingModified.class, correlationId, thingId);
                    assertThat(thingModified.getRevision()).isEqualTo(thingCreatedRevision.get() + 1L);
                    sendSignal(cf.connectionName2, Acknowledgement.of(customAck, thingId, HttpStatus.REQUEST_TIMEOUT,
                            thingModified.getDittoHeaders()));
                },
                // THEN: the command is redelivered, generating another event, but the correlation ID may change.
                eventFromRedelivery -> {
                    final ThingModified thingModified =
                            thingEventForJson(eventFromRedelivery, ThingModified.class, null, thingId);
                    assertThat(thingModified.getRevision()).isEqualTo(thingCreatedRevision.get() + 2L);
                    sendSignal(cf.connectionName2, Acknowledgement.of(customAck, thingId, HttpStatus.OK,
                            thingModified.getDittoHeaders()));
                }
                ),
                "ThingCreated",
                "initial ThingModified",
                "redelivered ThingModified"
        );
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1, CONNECTION2})
    public void weakAckForSourceDeclaredAckIsIssuedAutomaticallyForConnectionsWithoutExtraFields()
            throws InterruptedException {
        // Given
        final String thingCreatedCorrelationId = createNewCorrelationId();
        final C eventConsumer = initTargetsConsumer(cf.connectionName2);
        final ThingId randomThingId = generateThingId();
        final ThingId thingId = ThingId.of(randomThingId.getNamespace(), randomThingId.getName() + "-filter-events");
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(putPolicyForThing(thingId, cf.connectionName1, cf.connectionName2))
                .build();
        final AcknowledgementRequest twinPersistedAckRequest =
                AcknowledgementRequest.of(DittoAcknowledgementLabel.TWIN_PERSISTED);
        final AcknowledgementLabel customAck = connectionScopedAckLabel(cf.connectionName2, "custom", cf);
        final AcknowledgementRequest customAckRequest = AcknowledgementRequest.of(customAck);
        final AcknowledgementLabel hopelessAck = AcknowledgementLabel.of("test");

        final CountDownLatch eventLatch = new CountDownLatch(1);
        consumeAndAssertEventsInFuture(cf.connectionName2, eventConsumer, List.of(created -> eventLatch.countDown()),
                "ThingCreated"
        );

        final Response response = putThing(2, thing, JsonSchemaVersion.V_2)
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                        String.join(",", twinPersistedAckRequest.getLabel(), customAckRequest.getLabel(), hopelessAck))
                .withHeader(DittoHeaderDefinition.TIMEOUT.getKey(), "2s")
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .withCorrelationId(thingCreatedCorrelationId)
                .fire();

        final JsonObject acknowledgements = JsonObject.of(response.body().prettyPrint());
        final Set<String> acknowledgementLabels =
                acknowledgements.getKeys().stream().map(Objects::toString).collect(Collectors.toSet());
        assertThat(acknowledgementLabels).hasSize(3);
        assertThat(acknowledgementLabels)
                .containsExactlyInAnyOrder(DittoAcknowledgementLabel.TWIN_PERSISTED.toString(), customAck.toString(),
                        hopelessAck.toString());
        final JsonPointer twinPersistedStatusKey =
                JsonPointer.of(DittoAcknowledgementLabel.TWIN_PERSISTED + "/status");
        assertThat(acknowledgements.getValue(twinPersistedStatusKey)).contains(JsonValue.of(201));
        final JsonPointer hopelessAckStatusKey = JsonPointer.of(hopelessAck + "/status");
        assertThat(acknowledgements.getValue(hopelessAckStatusKey)).contains(JsonValue.of(408));
        final JsonPointer customAckStatusKey = JsonPointer.of(customAck + "/status");
        assertThat(acknowledgements.getValue(customAckStatusKey)).contains(JsonValue.of(200));
        final JsonPointer isWeakAckKey =
                JsonPointer.of(customAck + "/headers/" + DittoHeaderDefinition.WEAK_ACK.getKey());
        assertThat(acknowledgements.getValue(isWeakAckKey)).contains(JsonValue.of(true));
        assertThat(eventLatch.await(5, TimeUnit.SECONDS)).isFalse();
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1, CONNECTION_WITH_EXTRA_FIELDS})
    public void weakAckForSourceDeclaredAckIsIssuedAutomaticallyForConnectionsWithExtraFields()
            throws InterruptedException {
        // Given
        final String thingCreatedCorrelationId = createNewCorrelationId();
        final C eventConsumer = initTargetsConsumer(cf.connectionNameWithExtraFields);
        final ThingId randomThingId = generateThingId();
        final ThingId thingId = ThingId.of(randomThingId.getNamespace(), randomThingId.getName() + "-filter-events");
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(putPolicyForThing(thingId, cf.connectionName1, cf.connectionNameWithExtraFields))
                .build();
        final AcknowledgementRequest twinPersistedAckRequest =
                AcknowledgementRequest.of(DittoAcknowledgementLabel.TWIN_PERSISTED);
        final AcknowledgementLabel customAck = connectionScopedAckLabel(cf.connectionNameWithExtraFields, "custom", cf);
        final AcknowledgementRequest customAckRequest = AcknowledgementRequest.of(customAck);
        final AcknowledgementLabel hopelessAck = AcknowledgementLabel.of("test");

        final CountDownLatch eventLatch = new CountDownLatch(1);
        consumeAndAssertEventsInFuture(cf.connectionNameWithExtraFields, eventConsumer,
                List.of(created -> eventLatch.countDown()), "ThingCreated");

        final Response response = putThing(2, thing, JsonSchemaVersion.V_2)
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                        String.join(",", twinPersistedAckRequest.getLabel(), customAckRequest.getLabel(), hopelessAck))
                .withHeader(DittoHeaderDefinition.TIMEOUT.getKey(), "2s")
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .withCorrelationId(thingCreatedCorrelationId)
                .fire();

        final JsonObject acknowledgements = JsonObject.of(response.body().prettyPrint());
        final Set<String> acknowledgementLabels =
                acknowledgements.getKeys().stream().map(Objects::toString).collect(Collectors.toSet());
        assertThat(acknowledgementLabels).hasSize(3);
        assertThat(acknowledgementLabels)
                .containsExactlyInAnyOrder(DittoAcknowledgementLabel.TWIN_PERSISTED.toString(), customAck.toString(),
                        hopelessAck.toString());
        final JsonPointer twinPersistedStatusKey =
                JsonPointer.of(DittoAcknowledgementLabel.TWIN_PERSISTED + "/status");
        assertThat(acknowledgements.getValue(twinPersistedStatusKey)).contains(JsonValue.of(201));
        final JsonPointer hopelessAckStatusKey = JsonPointer.of(hopelessAck + "/status");
        assertThat(acknowledgements.getValue(hopelessAckStatusKey)).contains(JsonValue.of(408));
        final JsonPointer customAckStatusKey = JsonPointer.of(customAck + "/status");
        assertThat(acknowledgements.getValue(customAckStatusKey)).contains(JsonValue.of(200));
        final JsonPointer isWeakAckKey =
                JsonPointer.of(customAck + "/headers/" + DittoHeaderDefinition.WEAK_ACK.getKey());
        assertThat(acknowledgements.getValue(isWeakAckKey)).contains(JsonValue.of(true));
        assertThat(eventLatch.await(5, TimeUnit.SECONDS)).isFalse();
    }

    @Test
    @Connections({CONNECTION1, CONNECTION2})
    public void weakAckForTargetsIssuedAutomatically() {
        // GIVEN: An authorized connection exists for the ThingCreated event: otherwise Things issues weak ack directly
        final String thingCreatedCorrelationId = createNewCorrelationId();
        final ThingId thingId = generateThingId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(putPolicyForThing(thingId, cf.connectionName1))
                .build();
        final AcknowledgementLabel targetIssuedAck =
                ConnectionModelFactory.toAcknowledgementLabel(cf.getConnectionId(cf.connectionName2),
                        cf.defaultTargetAddress(cf.connectionName2));

        // WHEN: The CreateThing command requests the acknowledgement of an unauthorized connection
        final Response response = putThing(2, thing, JsonSchemaVersion.V_2)
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                        targetIssuedAck + "," + DittoAcknowledgementLabel.TWIN_PERSISTED)
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .withCorrelationId(thingCreatedCorrelationId)
                .fire();

        // THEN
        final JsonObject acknowledgements = JsonObject.of(response.body().asString());
        final Set<String> acknowledgementLabels =
                acknowledgements.getKeys().stream().map(Objects::toString).collect(Collectors.toSet());
        assertThat(acknowledgementLabels).hasSize(2);
        final JsonPointer twinPersistedStatusKey =
                JsonPointer.of(DittoAcknowledgementLabel.TWIN_PERSISTED + "/status");
        assertThat(acknowledgements.getValue(twinPersistedStatusKey)).contains(JsonValue.of(201));
        final JsonPointer customAckStatusKey = JsonPointer.of(targetIssuedAck + "/status");
        assertThat(acknowledgements.getValue(customAckStatusKey)).contains(JsonValue.of(200));
        final JsonPointer isWeakAckKey =
                JsonPointer.of(targetIssuedAck + "/headers/" + DittoHeaderDefinition.WEAK_ACK.getKey());
        assertThat(acknowledgements.getValue(isWeakAckKey)).contains(JsonValue.of(true));
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1, CONNECTION2})
    public void sendAckWhichHasNotBeenDeclared() {
        // Given
        final String correlationId = createNewCorrelationId();
        final ThingId thingId = generateThingId();

        // Given: ModifyThing with custom requested ack is sent
        final C responseConsumer = initResponseConsumer(cf.connectionName2, correlationId);
        final CompletableFuture<Void> consumedFuture =
                consumeAndAssertEventsInFuture(cf.connectionName2, responseConsumer, List.of(error -> {
                    assertThat(error).isInstanceOf(ThingErrorResponse.class);
                    final DittoRuntimeException reason = ((ThingErrorResponse) error).getDittoRuntimeException();
                    assertThat(reason).isInstanceOf(AcknowledgementLabelNotDeclaredException.class);
                }));
        final AcknowledgementLabel customAck = connectionScopedAckLabel(cf.connectionName2, "notDeclared", cf);
        final Acknowledgement acknowledgement = Acknowledgement.of(customAck, thingId, HttpStatus.REQUEST_TIMEOUT,
                DittoHeaders.newBuilder().correlationId(correlationId).build());
        sendSignal(cf.connectionName2, acknowledgement);
        consumedFuture.join();
    }

    @Test
    // Not available for target only connections as it is required to respond to the live message
    @Category(RequireSource.class)
    @Connections({CONNECTION1, CONNECTION2})
    public void sendTwinCommandAndLiveMessageRequestingTargetAcknowledgement() {
        // GIVEN: policy of thing authorizes connection 1
        final String correlationId = createNewCorrelationId();
        final ThingId thingId = generateThingId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(putPolicyForThing(thingId, cf.connectionName1))
                .build();

        final AcknowledgementLabel twinPersisted = DittoAcknowledgementLabel.TWIN_PERSISTED;
        final AcknowledgementLabel customAck = ConnectionModelFactory.toAcknowledgementLabel(
                cf.getConnectionId(cf.connectionName1), cf.defaultTargetAddress(cf.connectionName1));

        // WHEN: Thing is created via HTTP requesting automatic acknowledgement from target
        // THEN: Response is received right away
        final C targetConsumer = initTargetsConsumer(cf.connectionName1);
        final Response response = putThing(2, thing, JsonSchemaVersion.V_2)
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .withCorrelationId(correlationId)
                .withHeader("requested-acks", twinPersisted + "," + customAck)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        // THEN: Event is published at target
        consumeAndAssertEvents(cf.connectionName1, targetConsumer, Collections.singletonList(
                e -> {
                    final ThingCreated tc = thingEventForJson(e, ThingCreated.class, correlationId, thingId);
                    assertThat(tc.getRevision()).isEqualTo(1L);
                }));

        // THEN: response consists of aggregated acknowledgements
        final JsonObject jsonObject = JsonObject.of(response.body().asString());
        assertThat(jsonObject).containsKey(twinPersisted, customAck);
        assertThat(jsonObject.getValue(customAck).orElseThrow().asObject()
                .getValue("headers").orElseThrow().asObject())
                .contains(JsonKey.of(DittoHeaderDefinition.CONNECTION_ID.getKey()),
                        JsonValue.of(cf.getConnectionId(cf.connectionName1).toString()));

        final String featureId = "feature";
        final String subject = "topic";
        final String messageCorrelationId = UUID.randomUUID().toString();

        final CompletableFuture<Void> messageConsumedFuture =
                consumeAndAssertEventsInFuture(cf.connectionName1, targetConsumer,
                        List.of(signal -> {
                            assertThat(signal).isInstanceOf(SendFeatureMessage.class);
                            final SendFeatureMessage<?> messageCommand = (SendFeatureMessage<?>) signal;
                            assertThat(messageCommand.getFeatureId()).isEqualTo(featureId);
                            assertThat(messageCommand.getMessage().getSubject()).isEqualTo(subject);
                            assertThat(messageCommand.getMessage().getPayload().<Object>map(x -> x).orElse(""))
                                    .isEqualTo("hello");
                            sendBackFeatureMessageResponse(thingId, featureId, subject, messageCorrelationId,
                                    "application/json", "{\"success\":true}", HttpStatus.OK);

                        }),
                        "liveMessage");

        // WHEN: a message is sent via HTTP requesting automatic acknowledgement from target
        // THEN: response is sent immediately containing the requested acknowledgement
        expect().statusCode(anyOf(is(200), is(204))) // status is 204 for Kafka and 200 for others
                .given()
                .header("Authorization",
                        "Bearer " + testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .header(HttpHeader.CONTENT_TYPE.getName(), "text/plain")
                .header("x-correlation-id", messageCorrelationId)
                .header("requested-acks", customAck.toString())
                .body("hello")
                .when()
                .post(messagesResourceForToFeatureV2(thingId, featureId, subject));

        // THEN: message is published
        messageConsumedFuture.join();
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1, CONNECTION2})
    public void nAckWillNotBeDeliveredAsResponseByDefault() {
        // Given
        final String correlationId = createNewCorrelationId();
        final ThingId thingId = generateThingId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
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

        final AcknowledgementRequest twinPersistedAckRequest =
                AcknowledgementRequest.of(DittoAcknowledgementLabel.TWIN_PERSISTED);
        final AcknowledgementLabel customAck = connectionScopedAckLabel(cf.connectionName2, "custom", cf);
        final AcknowledgementRequest customAckRequest = AcknowledgementRequest.of(customAck);
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), createDittoHeaders(correlationId)
                .toBuilder()
                .acknowledgementRequest(twinPersistedAckRequest, customAckRequest)
                .timeout(Duration.ofSeconds(3))
                .build());

        // When
        final C responseConsumer = initResponseConsumer(cf.connectionName1, correlationId);
        final C eventConsumer = initTargetsConsumer(cf.connectionName2);

        sendSignal(cf.connectionName1, createThing);

        consumeAndAssertEvents(cf.connectionName2, eventConsumer, Collections.singletonList(
                e -> {
                    final ThingCreated tc = thingEventForJson(e, ThingCreated.class, correlationId, thingId);
                    assertThat(tc.getRevision()).isEqualTo(1L);
                    assertThat(tc.getDittoHeaders().getAcknowledgementRequests()).contains(customAckRequest);
                    assertThat(tc.getDittoHeaders().getAcknowledgementRequests())
                            .doesNotContain(twinPersistedAckRequest);
                    sendSignal(cf.connectionName2, Acknowledgement.of(customAck, createThing.getEntityId(),
                            HttpStatus.REQUEST_TIMEOUT, createThing.getDittoHeaders()
                    ));
                }));

        final M response = consumeResponse(correlationId, responseConsumer);
        sendSignal(cf.connectionName2, Acknowledgement.of(customAck, createThing.getEntityId(), HttpStatus.OK,
                createThing.getDittoHeaders()
        ));
        /*
         * we get a response because create thing command was negatively acknowledged. This response is however not the
         * N_ACK response, but the error response that the policy with this ID already exists, because the same CreateThing
         * was redelivered.
         */
        assertThat(response).isNotNull();
        assertThat(getCorrelationId(response)).isEqualTo(correlationId);

        final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(response);
        final Adaptable eventAdaptable1 = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
        final Jsonifiable<JsonObject> theResponse = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable1);
        assertThat(theResponse).isInstanceOf(ThingErrorResponse.class);
        final ThingErrorResponse errorResponse = (ThingErrorResponse) theResponse;
        assertThat(errorResponse.getDittoRuntimeException()).isInstanceOf(ThingNotCreatableException.class);
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1, CONNECTION2})
    public void nAckIsDeliveredIfReplyTargetExpectsIt() {
        // Given
        final String correlationId = createNewCorrelationId();
        final ThingId thingId = generateThingId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(PolicyId.of(thingId))
                .build();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(cf.connectionName2))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .forLabel("RESTRICTED")
                .setSubject(connectionSubject(cf.connectionName1))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ)
                .build();

        final AcknowledgementRequest twinPersistedAckRequest =
                AcknowledgementRequest.of(DittoAcknowledgementLabel.TWIN_PERSISTED);
        final AcknowledgementLabel customAck = connectionScopedAckLabel(cf.connectionName1, "custom", cf);
        final AcknowledgementRequest customAckRequest = AcknowledgementRequest.of(customAck);
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), createDittoHeaders(correlationId)
                .toBuilder()
                .acknowledgementRequest(twinPersistedAckRequest, customAckRequest)
                .timeout(Duration.ofSeconds(3))
                .build());

        // When
        final C responseConsumer = initResponseConsumer(cf.connectionName2, correlationId);
        final C eventConsumer = initTargetsConsumer(cf.connectionName1);

        sendSignal(cf.connectionName2, createThing);

        consumeAndAssertEvents(cf.connectionName1, eventConsumer, Collections.singletonList(
                e -> {
                    final ThingCreated tc = thingEventForJson(e, ThingCreated.class, correlationId, thingId);
                    assertThat(tc.getRevision()).isEqualTo(1L);
                    assertThat(tc.getDittoHeaders().getAcknowledgementRequests()).contains(customAckRequest);
                    assertThat(tc.getDittoHeaders().getAcknowledgementRequests())
                            .doesNotContain(twinPersistedAckRequest);
                    sendSignal(cf.connectionName1, Acknowledgement.of(customAck, createThing.getEntityId(),
                            HttpStatus.BAD_REQUEST, createThing.getDittoHeaders()
                    ));
                }));

        final M response = consumeResponse(correlationId, responseConsumer);
        assertThat(response).isNotNull();
        assertThat(getCorrelationId(response)).isEqualTo(correlationId);

        final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(response);
        final Adaptable eventAdaptable1 = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
        final Jsonifiable<JsonObject> theResponse = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable1);
        assertThat(theResponse).isInstanceOf(Acknowledgements.class);

        final Acknowledgements acks = (Acknowledgements) theResponse;
        assertThat(acks.getHttpStatus()).isEqualTo(HttpStatus.FAILED_DEPENDENCY);
        assertThat(acks.getSize()).isEqualTo(2);
        final Set<Acknowledgement> successfulAcknowledgements = acks.getSuccessfulAcknowledgements();
        assertThat(successfulAcknowledgements.stream()
                .map(Acknowledgement::getLabel)
        ).containsExactly(twinPersistedAckRequest.getLabel());

        assertThat(filterByLabel(successfulAcknowledgements, twinPersistedAckRequest.getLabel())
                .map(Acknowledgement::getHttpStatus)
                .findAny()
        ).contains(HttpStatus.CREATED);
        assertThat(filterByLabel(successfulAcknowledgements, twinPersistedAckRequest.getLabel())
                .map(Acknowledgement::getEntity)
                .flatMap(Optional::stream)
                .map(JsonValue::asObject)
                .map(ThingsModelFactory::newThing)
                .findAny()
        ).contains(thing);

        final Set<AcknowledgementLabel> failedAcknowledgements = acks.getFailedAcknowledgements()
                .stream()
                .map(Acknowledgement::getLabel)
                .collect(Collectors.toSet());
        assertThat(failedAcknowledgements).containsExactly(customAckRequest.getLabel());
    }

    private static Stream<Acknowledgement> filterByLabel(final Set<Acknowledgement> successfulAcknowledgements,
            final AcknowledgementLabel label) {
        return successfulAcknowledgements.stream().filter(ack -> ack.getLabel().equals(label));
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1})
    public void retrieveThings() {
        final ThingId thing1 = sendCreateThingAndEnsureResponseIsSentBack(cf.connectionName1);
        final ThingId thing2 = sendCreateThingAndEnsureResponseIsSentBack(cf.connectionName1);
        final ThingId thing3 = sendCreateThingAndEnsureResponseIsSentBack(cf.connectionName1);

        final RetrieveThings retrieveThings = RetrieveThings.getBuilder(thing1, thing2, thing3)
                .dittoHeaders(DittoHeaders.newBuilder().correlationId(UUID.randomUUID().toString()).build())
                .build();

        final CommandResponse<?> commandResponse =
                sendCommandAndEnsureResponseIsSentBack(cf.connectionName1, retrieveThings);
        assertThat(commandResponse).isInstanceOf(RetrieveThingsResponse.class);
        assertThat(((RetrieveThingsResponse) commandResponse).getThings()).hasSize(3);
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1})
    public void createPolicyAndThingWithAllowPolicyLockoutHeader() {
        final DittoHeaders allowLockout = DittoHeaders.newBuilder().allowPolicyLockout(true).build();
        final Permissions readWrite = PoliciesModelFactory.newPermissions("READ", "WRITE");
        final Permissions write = PoliciesModelFactory.newPermissions("WRITE");
        final Permissions read = PoliciesModelFactory.newPermissions("READ");
        final Permissions empty = PoliciesModelFactory.noPermissions();

        // create policy with RW on policy resource --> OK
        createPolicyAndAssertResult(HttpStatus.CREATED, readWrite);
        // create policy with W on policy resource --> OK
        createPolicyAndAssertResult(HttpStatus.CREATED, write);
        // create policy with R on policy resource --> FAIL (by default WRITE is required)
        createPolicyAndAssertResult(HttpStatus.FORBIDDEN, read);
        // create policy with [] on policy resource --> FAIL (by default WRITE is required)
        createPolicyAndAssertResult(HttpStatus.FORBIDDEN, empty);

        // create policy + allow header with RW on policy resource --> OK
        final EntityId readWriteOnPolicy = createPolicyAndAssertResult(allowLockout, HttpStatus.CREATED, readWrite);
        // create policy + allow header with WRITE on policy resource --> OK
        final EntityId writeOnPolicy = createPolicyAndAssertResult(allowLockout, HttpStatus.CREATED, write);
        // create policy + allow header with READ on policy resource --> OK
        final EntityId readOnPolicy = createPolicyAndAssertResult(allowLockout, HttpStatus.CREATED, read);
        // create policy + allow header with NO permission on policy resource --> OK
        final EntityId noPermissionPolicy = createPolicyAndAssertResult(allowLockout, HttpStatus.CREATED, empty);

        // first create some things with policy reference with ENABLED policy lockout prevention (default)
        // create a thing with policy reference using policy with RW --> OK
        createNewThingWithPolicyRef(randomThing(), readWriteOnPolicy.toString(), cf.connectionName1, 201);
        // create a thing with policy reference using policy with WRITE --> FAIL (READ is required to read policy)
        createNewThingWithPolicyRef(randomThing(), writeOnPolicy.toString(), cf.connectionName1, 404);
        // create a thing with policy reference using policy with READ --> FAIL (policy lockout prevention)
        createNewThingWithPolicyRef(randomThing(), readOnPolicy.toString(), cf.connectionName1, 403);
        // create a thing with policy reference using policy with no permission on policy resource -> FAIL
        createNewThingWithPolicyRef(randomThing(), noPermissionPolicy.toString(), cf.connectionName1, 404);

        // then create some things with policy reference with DISABLED policy lockout prevention
        // create a thing with policy reference using policy with RW --> OK
        createNewThingWithPolicyRef(randomThing(), readWriteOnPolicy.toString(), cf.connectionName1, allowLockout, 201);
        // create a thing with policy reference using policy with WRITE --> FAIL (READ is required to read policy)
        createNewThingWithPolicyRef(randomThing(), writeOnPolicy.toString(), cf.connectionName1, allowLockout, 404);
        // create a thing with policy reference using policy with READ --> OK (policy lockout prevention bypassed)
        createNewThingWithPolicyRef(randomThing(), readOnPolicy.toString(), cf.connectionName1, allowLockout, 201);
        // create a thing with policy reference using policy with no permission on policy resource -> FAIL
        createNewThingWithPolicyRef(randomThing(), noPermissionPolicy.toString(), cf.connectionName1, allowLockout,
                404);
    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1, CONNECTION2})
    public void sendMergeCommandsAndEnsureEventsAreProduced() {
        // Given
        final String correlationId = createNewCorrelationId();
        final ThingId thingId = generateThingId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
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

        final String featureId = "coffeebrewer";
        final Attributes attributes = Attributes.newBuilder().set("newAttribute", "attrValue").build();
        final FeatureProperties properties = FeatureProperties.newBuilder().set("brewed-coffees", 42).build();
        final Feature feature = Feature.newBuilder().properties(properties).withId(featureId).build();
        final Features features = Features.newBuilder().set(feature).build();
        final Thing thingToMerge = thing.toBuilder().setAttributes(attributes).setFeatures(features).build();
        final MergeThing mergeThing = MergeThing.withThing(thingId, thingToMerge, createDittoHeaders(correlationId));

        // When
        final C eventConsumer = initTargetsConsumer(cf.connectionName2);
        putThingWithPolicy(2, thing, policy, JsonSchemaVersion.V_2)
                .withCorrelationId(correlationId)
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final CommandResponse<?> commandResponse =
                sendCommandAndEnsureResponseIsSentBack(cf.connectionName1, mergeThing);
        LOGGER.info("Received response: {}", commandResponse);
        assertThat(commandResponse).isInstanceOf(MergeThingResponse.class);
        assertThat(commandResponse.getHttpStatus()).isEqualTo(HttpStatus.NO_CONTENT);

        consumeAndAssertEvents(cf.connectionName2, eventConsumer, Arrays.asList(
                e -> {
                    LOGGER.info("Received event: {}", e);
                    final ThingCreated tc = thingEventForJson(e, ThingCreated.class, correlationId, thingId);
                    assertThat(tc.getRevision()).isEqualTo(1L);
                },
                e -> {
                    LOGGER.info("Received event: {}", e);
                    final ThingMerged tc = thingEventForJson(e, ThingMerged.class, correlationId, thingId);
                    assertThat(tc.getRevision()).isEqualTo(2L);
                }), "ThingCreated", "ThingMerged");

    }

    @Test
    @Category(RequireSource.class)
    @Connections({CONNECTION1})
    public void sendMergeThingCommandWithIncorrectContentTypeHeader() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final TopicPath topicPath = TopicPath.newBuilder(thingId).things().twin().commands().merge().build();
        final String correlationId = UUID.randomUUID().toString();
        final String createFeatureWithMultipleSlashesInPath = "{" +
                "\"topic\": \"" + topicPath.getPath() + "\", " +
                "\"path\": \"/attributes/attr1\", " +
                "\"headers\":{\"content-type\":\"application/json\",\"version\":1,\"correlation-id\":\"" +
                correlationId + "\"}," +
                "\"payload\": 42" +
                "}";

        // When
        final C consumer = initResponseConsumer(cf.connectionName1, correlationId);
        sendAsJsonString(cf.connectionName1, correlationId, createFeatureWithMultipleSlashesInPath,
                DittoHeaders.newBuilder().correlationId(correlationId).build());

        // Then
        final M message = consumeResponse(correlationId, consumer);
        assertThat(message).isNotNull();
        assertThat(getCorrelationId(message)).isEqualTo(correlationId);

        final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(message);
        final Adaptable eventAdaptable1 = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
        final Jsonifiable<JsonObject> event = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable1);
        assertThat(event).isInstanceOf(ThingErrorResponse.class);
        assertThat(((ThingErrorResponse) event).getDittoRuntimeException().getMessage())
                .contains("The Media-Type <application/json> is not supported for this resource.");

    }

    @Test
    @Connections(CONNECTION_WITH_CONNECTION_ANNOUNCEMENTS)
    public void sendsConnectionAnnouncements() {
        final String connectionName = cf.connectionWithConnectionAnnouncements;
        final String connectionId = cf.getConnectionId(connectionName).toString();
        final C consumer = initTargetsConsumer(connectionName);

        // some connection types will still receive the opened announcement after initializing the targets consumer,
        // some targets will already have received it before initializing the target in this test. Therefore they
        // either receive an opened announcement or nothing
        expectMsgClassOrNothing(ConnectionOpenedAnnouncement.class, connectionName, consumer);

        try {
            connectionsClient().closeConnection(connectionId)
                    .withDevopsAuth()
                    .expectingHttpStatus(HttpStatus.OK)
                    .fire();

            expectMsgClass(ConnectionClosedAnnouncement.class, connectionName, consumer);
        } finally {
            connectionsClient().openConnection(connectionId)
                    .withDevopsAuth()
                    .expectingHttpStatus(HttpStatus.OK)
                    .fire();
            expectMsgClass(ConnectionOpenedAnnouncement.class, connectionName, consumer);
        }
    }

    /**
     * This tests only exists to tests each connectivity channel once.
     * For more specific connectivity tests on the 'condition' feature, see:
     * {@link org.eclipse.ditto.testing.system.conditional.ConditionAmqpIT}.
     */
    @Test
    @Category(RequireSource.class)
    @Connections(CONNECTION1)
    public void updateAttributeBasedOnCondition() {
        final String correlationId = createNewCorrelationId();
        final ThingId thingId = generateThingId();
        final Thing thing = new ThingJsonProducer().getThing().toBuilder()
                .setId(thingId)
                .build();

        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(cf.connectionName1))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();

        putThingWithPolicy(2, thing, policy, JsonSchemaVersion.V_2)
                .withCorrelationId(correlationId)
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final DittoHeaders headers = DittoHeaders.newBuilder()
                .correlationId(correlationId)
                .condition("eq(attributes/manufacturer,\"ACME\")")
                .build();

        final ModifyAttribute modifyAttribute = ModifyAttribute.of(thingId, JsonPointer.of("manufacturer"),
                JsonFactory.newValue("Bosch IO"), headers);

        final CommandResponse<?> commandResponse =
                sendCommandAndEnsureResponseIsSentBack(cf.connectionName1, modifyAttribute);
        assertThat(commandResponse).isInstanceOf(ModifyAttributeResponse.class);
    }

    @Test
    @Category(RequireSource.class)
    @Connections(CONNECTION1)
    public void updateAttributeBasedOnConditionFailsWithConditionFailedException() {
        final String correlationId = createNewCorrelationId();
        final ThingId thingId = sendCreateThingAndEnsureResponseIsSentBack(cf.connectionName1);

        final DittoHeaders headers = DittoHeaders.newBuilder()
                .correlationId(correlationId)
                .condition("eq(attributes/manufacturer,\"Robert Bosch\")")
                .build();

        final ModifyAttribute modifyAttribute = ModifyAttribute.of(thingId, JsonPointer.of("manufacturer"),
                JsonFactory.newValue("Bosch IO"), headers);

        final ErrorResponse<?> response =
                sendSignalAndExpectError(modifyAttribute, cf.connectionName1, HttpStatus.PRECONDITION_FAILED,
                        correlationId);

        assertThat(response).isInstanceOf(ThingErrorResponse.class);
        final ThingErrorResponse errorResponse = (ThingErrorResponse) response;
        assertThat(errorResponse.getDittoRuntimeException()).isInstanceOf(ThingConditionFailedException.class);
    }

    @Test
    @Category(RequireSource.class)
    @Connections(CONNECTION1)
    public void updateAttributeBasedOnConditionFailsForInvalidCondition() {
        final String correlationId = createNewCorrelationId();
        final ThingId thingId = sendCreateThingAndEnsureResponseIsSentBack(cf.connectionName1);

        final DittoHeaders headers = DittoHeaders.newBuilder()
                .correlationId(correlationId)
                .condition("eq(attributes/manufacturer,\"Robert Bosch\"")
                .build();

        final ModifyAttribute modifyAttribute = ModifyAttribute.of(thingId, JsonPointer.of("manufacturer"),
                JsonFactory.newValue("Bosch IO"), headers);

        final ErrorResponse<?> response =
                sendSignalAndExpectError(modifyAttribute, cf.connectionName1, HttpStatus.BAD_REQUEST,
                        correlationId);

        assertThat(response).isInstanceOf(ThingErrorResponse.class);
        final ThingErrorResponse errorResponse = (ThingErrorResponse) response;
        assertThat(errorResponse.getDittoRuntimeException()).isInstanceOf(ThingConditionInvalidException.class);
    }

    @Test
    @Connections({ConnectionCategory.CONNECTION1})
    public void eventsAreNotDeliveredForSubjectsWithRevokeInHierarchy() {
        final var connection = cf.connectionName1;

        // Given
        final var featureId = "Lamp";
        final var thingId = generateThingId();
        final var policyId = PolicyId.of(thingId);

        final var thing = Thing.newBuilder()
                .setId(thingId)
                .setPolicyId(policyId)
                .setFeature(Feature.newBuilder()
                        .withId(featureId)
                        .build())
                .build();

        final var permissions = Permissions.newInstance("READ", "WRITE");
        final var policy = Policy.newBuilder(policyId)
                .forLabel("connections-grant")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(connection))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), permissions)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), permissions)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), permissions)
                .forLabel("connections-revoke")
                .setSubject(connectionSubject(connection))
                .setRevokedPermissions(PoliciesResourceType.thingResource("/features/" + featureId), permissions)
                .build();

        putPolicy(policyId, policy)
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final var consumer = initTargetsConsumer(connection);

        // When Thing is created
        final var createThingCorrelationId = createNewCorrelationId();
        putThing(2, thing, JsonSchemaVersion.V_2)
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .withCorrelationId(createThingCorrelationId)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final var message = consumeFromTarget(connection, consumer);
        assertThat(message).isNull();
    }

    @Test
    @Category(RequireSource.class)
    @UseConnection(category = ConnectionCategory.CONNECTION1, mod = EMPTY_MOD)
    @UseConnection(category = ConnectionCategory.CONNECTION2, mod = FILTER_FOR_LIFECYCLE_EVENTS_BY_RQL)
    public void subscribeOnlyForLifecycleEvents() {
        final CreateThing createThingWithPolicy = newCreateThing(cf.connectionName1, cf.connectionName2);
        final String correlationId = createThingWithPolicy.getDittoHeaders().getCorrelationId().orElseThrow();
        final ThingId thingId = createThingWithPolicy.getEntityId();

        final JsonPointer modAttrPointer = JsonPointer.of("modAttr");
        final JsonValue modAttrValue = JsonValue.of(42);

        final C eventConsumer = initTargetsConsumer(cf.connectionName2);

        sendSignal(cf.connectionName1, createThingWithPolicy);

        consumeAndAssertEvents(cf.connectionName2, eventConsumer, Collections.singletonList(
                e -> {
                    final ThingCreated tc = thingEventForJson(e, ThingCreated.class, correlationId, thingId);
                    assertThat(tc.getRevision()).isEqualTo(1L);
                }), "ThingCreated");

        final ModifyAttribute modifyAttribute =
                ModifyAttribute.of(thingId, modAttrPointer, modAttrValue, createDittoHeaders(UUID.randomUUID().toString()));
        final MergeThing mergeThing =
                MergeThing.of(thingId, JsonPointer.of("attributes/test"), modAttrValue,
                        createDittoHeaders(UUID.randomUUID().toString()));
        final ModifyFeature modifyFeature = ModifyFeature.of(thingId, Feature.newBuilder().properties(FeatureProperties
                .newBuilder()
                .set("one", 2)
                .build()
        ).withId("new").build(), createDittoHeaders(UUID.randomUUID().toString()));
        sendSignal(cf.connectionName1, modifyAttribute);
        sendSignal(cf.connectionName1, mergeThing);
        sendSignal(cf.connectionName1, modifyFeature);

        final M attributeModifiedEvent = consumeFromTarget(cf.connectionName2, eventConsumer);
        assertThat(attributeModifiedEvent).as("Did not expect to receive any modified/merged events")
                .isNull();

        final String deletionCid = UUID.randomUUID().toString();
        final DeleteThing deleteThing = DeleteThing.of(thingId, DittoHeaders.newBuilder()
                .correlationId(deletionCid)
                .build());
        sendSignal(cf.connectionName1, deleteThing);

        consumeAndAssertEvents(cf.connectionName2, eventConsumer, Collections.singletonList(
                e -> {
                    final ThingDeleted tc = thingEventForJson(e, ThingDeleted.class, deletionCid, thingId);
                    assertThat(tc.getRevision()).isEqualTo(5L);
                }), "ThingDeleted");
    }

    private static Thing randomThing() {
        return Thing.newBuilder().setGeneratedId().build();
    }

    private EntityId createPolicyAndAssertResult(final HttpStatus status, final Permissions policyGrants) {
        return createPolicyAndAssertResult(DittoHeaders.empty(), status, policyGrants);
    }

    private EntityId createPolicyAndAssertResult(final DittoHeaders headers, final HttpStatus status,
            final Permissions policyGrants) {

        final Policy policy = createNewPolicy(generatePolicyId(), policyGrants);
        final String correlationId = UUID.randomUUID().toString();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder(headers).correlationId(correlationId).build();
        final CreatePolicy createPolicy = CreatePolicy.of(policy, dittoHeaders);

        final CommandResponse<?> commandResponse =
                sendCommandAndEnsureResponseIsSentBack(cf.connectionName1, createPolicy);
        assertThat(commandResponse.getHttpStatus()).isEqualTo(status);
        if (HttpStatus.CREATED.equals(status)) {
            assertThat(commandResponse).isInstanceOf(CreatePolicyResponse.class);
            assertThat(((CreatePolicyResponse) commandResponse).getPolicyCreated()
                    .flatMap(Policy::getEntityId))
                    .isEqualTo(policy.getEntityId());
        }
        assertThat(commandResponse).isInstanceOf(WithEntityId.class);
        return ((WithEntityId) commandResponse).getEntityId();
    }

    private Policy createNewPolicy(final EntityId entityId, final Permissions policyPermissions) {
        return Policy.newBuilder(PolicyId.of(entityId))
                .forLabel("INTEGRATION")
                .setSubject(connectionSubject(cf.connectionName1))
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), policyPermissions)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), "READ", "WRITE")
                .forLabel("ADMIN")
                .setSubject(ThingsSubjectIssuer.DITTO, "admin")
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), "READ", "WRITE")
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), "READ", "WRITE")
                .build();
    }

}
