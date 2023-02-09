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
import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.entity.id.EntityId;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.json.Jsonifiable;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.commands.Command;
import org.eclipse.ditto.base.model.signals.commands.CommandResponse;
import org.eclipse.ditto.base.model.signals.commands.ErrorResponse;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.Permissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.testing.common.IdGenerator;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.ThingErrorResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThingResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttribute;
import org.eclipse.ditto.things.model.signals.events.ThingCreated;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractConnectivityITCommon<C, M> extends AbstractConnectivityITBase<C, M> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnectivityITestCases.class);

    protected static final int DEFAULT_MAX_THING_SIZE = 1024 * 100;

    protected static final String DEVICE_ID_HEADER = "device_id";

    @Rule
    public TestName name = new TestName();

    @Rule
    public final TestWatcher watchman = new DefaultTestWatcher(LOGGER);

    @Rule
    public ConnectionsWatcher connectionsWatcher = new ConnectionsWatcher();

    protected AbstractConnectivityITCommon(final ConnectivityFactory cf) {
        super(cf);
    }

    protected abstract String targetAddressForTargetPlaceHolderSubstitution();

    protected ThingId generateThingId() {
        return generateThingId(randomNamespace);
    }

    protected PolicyId generatePolicyId() {
        return PolicyId.of(IdGenerator.fromNamespace(randomNamespace).withPrefixedRandomName(name.getMethodName()));
    }

    /**
     * @return whether the concrete test protocol supports sources. Tests depending on sources are skipped otherwise.
     */
    protected boolean connectionTypeSupportsSource() {
        return true;
    }

    /**
     * @return whether the concrete test protocol relies on headers in sent adaptables. (I.e. Mqtt5 tests rely on the
     * reply target set to the adaptable)
     */
    protected boolean connectionTypeHasHeadersInAdaptable() {
        return false;
    }

    protected ThingId generateThingId(final String namespace) {
        return ThingId.of(IdGenerator.fromNamespace(namespace).withPrefixedRandomName(name.getMethodName()));
    }

    protected static Map.Entry<String, String> entry(final String key, final String value) {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    protected final CommandResponse<?> sendCommandAndEnsureResponseIsSentBack(final String connectionName,
            final Command<?> command) {
        // Given
        final String correlationId =
                command.getDittoHeaders().getCorrelationId()
                        .orElseThrow(() -> new IllegalStateException("correlation-id is required."));

        // When
        final C consumer = initResponseConsumer(connectionName, correlationId);
        sendSignal(connectionName, command);

        // Then
        final M message = consumeResponse(correlationId, consumer);
        assertThat(message).isNotNull();
        assertThat(getCorrelationId(message)).isEqualTo(correlationId);
        checkHeaders(message);

        final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(message);
        final Adaptable responseAdaptable =
                ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
        final Jsonifiable<JsonObject> response = PROTOCOL_ADAPTER.fromAdaptable(responseAdaptable);

        assertThat(response).isNotNull();
        assertThat(response).isInstanceOf(CommandResponse.class);

        return (CommandResponse<?>) response;
    }

    protected CreateThing buildCreateThing(final String connectionName,
            final Function<ThingBuilder.FromScratch, Thing> thingModifier) {

        final ThingId thingId = generateThingId();
        final Thing thing = thingModifier.apply(Thing.newBuilder().setId(thingId));

        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(connectionName))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();

        final String correlationId = UUID.randomUUID().toString();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .correlationId(correlationId)
                .putHeader(DEVICE_ID_HEADER, thingId)
                .build();
        return CreateThing.of(thing, policy.toJson(), dittoHeaders);
    }

    protected final void sendCreateThing(final String connectionName,
            final Function<ThingBuilder.FromScratch, Thing> thingModifier) {

        final CreateThing createThing = buildCreateThing(connectionName, thingModifier);
        sendSignal(connectionName, createThing);
    }

    protected final ModifyAttribute modifyAttribute(final ThingId thingId, final JsonPointer attributeKey,
            final Object attributeValue) {

        final String correlationId = UUID.randomUUID().toString();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .correlationId(correlationId)
                .putHeader(DEVICE_ID_HEADER, thingId)
                .build();

        return ModifyAttribute.of(thingId, attributeKey, JsonValue.of(attributeValue), dittoHeaders);
    }

    @SafeVarargs // varargs not modified
    protected final void sendCreateThingAndExpectError(final Thing thing,
            final Policy policy,
            final String connectionName,
            final HttpStatus expectedResponseStatus,
            final Map.Entry<String, String>... additionalHeaders) {

        final String correlationId = UUID.randomUUID().toString();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .correlationId(correlationId)
                .putHeaders(
                        Stream.of(additionalHeaders).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                .build();

        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), dittoHeaders);

        final Jsonifiable<JsonObject> response =
                sendSignalAndExpectError(createThing, connectionName, expectedResponseStatus, correlationId);

        assertThat(response).isInstanceOf(ThingErrorResponse.class);
    }

    protected final ErrorResponse<?> sendSignalAndExpectError(final Signal<?> signal,
            final String connectionName,
            final HttpStatus expectedResponseStatus,
            final String correlationId) {

        // When
        final C consumer = initResponseConsumer(connectionName, correlationId);
        sendSignal(connectionName, signal);

        // Then
        final M message = consumeResponse(correlationId, consumer);
        assertThat(message).isNotNull();
        assertThat(getCorrelationId(message)).isEqualTo(correlationId);
        checkHeaders(message);

        final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(message);

        final Adaptable responseAdaptable = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
        final Jsonifiable<JsonObject> response = PROTOCOL_ADAPTER.fromAdaptable(responseAdaptable);

        assertThat(response).isNotNull();
        assertThat(response).isInstanceOf(ErrorResponse.class);
        assertThat(((CommandResponse<?>) response).getHttpStatus()).isEqualTo(expectedResponseStatus);

        return (ErrorResponse<?>) response;
    }

    protected void sendSingleCommandAndEnsureEventIsProduced(final String sendingConnectionName,
            final String receivingConnectionName) {
        sendSingleCommandAndEnsureEventIsProduced(sendingConnectionName, receivingConnectionName, () -> null);
    }

    protected <T> T sendSingleCommandAndEnsureEventIsProduced(final String sendingConnectionName,
            final String receivingConnectionName,
            final Supplier<T> resultAfterPolicyCreationSupplier) {
        // Given
        final CreateThing createThingWithPolicy = newCreateThing(sendingConnectionName, receivingConnectionName);
        final PolicyId policyId = PolicyId.of(createThingWithPolicy.getEntityId());
        final CreateThing createThing = CreateThing.of(
                createThingWithPolicy.getThing()
                        .toBuilder()
                        .setPolicyId(policyId)
                        .build(),
                null,
                createThingWithPolicy.getDittoHeaders());
        final String correlationId = createThing.getDittoHeaders().getCorrelationId().orElse(null);
        final ThingId thingId = createThing.getThing().getEntityId().orElse(null);

        // Given: Policy exists
        putPolicy(policyId, PoliciesModelFactory.newPolicy(createThingWithPolicy.getInitialPolicy().orElseThrow()))
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final T resultAfterPolicyCreation = resultAfterPolicyCreationSupplier.get();

        // When
        final C eventConsumer = initTargetsConsumer(receivingConnectionName);
        sendSignal(sendingConnectionName, createThing);

        // Then
        consumeAndAssert(receivingConnectionName, thingId, correlationId, eventConsumer, ThingCreated.class,
                tc -> assertThat(tc.getRevision()).isEqualTo(1L));

        return resultAfterPolicyCreation;
    }

    protected CreateThing newCreateThing(final String sendingConnectionName, final String receivingConnectionName) {
        final ThingId thingId = generateThingId();
        final PolicyId policyId = PolicyId.of(thingId);
        final Thing thing = Thing.newBuilder().setId(thingId).setPolicyId(policyId).build();
        final Policy policy = policyWithAccessFor(policyId, sendingConnectionName, receivingConnectionName);

        final JsonSchemaVersion schemaVersion = JsonSchemaVersion.V_2;
        final String correlationId = createNewCorrelationId();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .schemaVersion(schemaVersion)
                .correlationId(correlationId)
                .build();
        return CreateThing.of(thing, policy.toJson(), dittoHeaders);
    }

    protected Policy policyWithAccessFor(final PolicyId policyId, final String sendingConnectionName,
            final String receivingConnectionName) {
        final Permissions grantedPermissions = Permissions.newInstance("READ", "WRITE");
        return Policy.newBuilder(policyId)
                .forLabel("connections")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(sendingConnectionName))
                .setSubject(connectionSubject(receivingConnectionName))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), grantedPermissions)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), grantedPermissions)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), grantedPermissions)
                .build();
    }

    protected CreateThing newCreateThing(final String sendingConnectionName, final boolean responseRequired) {
        final ThingId thingId = generateThingId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(connectionSubject(sendingConnectionName))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();

        final String correlationId = createNewCorrelationId();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .correlationId(correlationId)
                .responseRequired(responseRequired)
                .build();
        return CreateThing.of(thing, policy.toJson(), dittoHeaders);
    }

    void createNewThing(final Thing thing, final Policy policy, final String connectionName) {
        new CreateNewThingWithPolicy(thing, policy, connectionName).fire();
    }

    void createNewThingWithPolicyRef(final Thing thing, final String placeholderOrId, final String connectionName,
            final DittoHeaders headers, final int expectedStatus) {
        new CreateNewThingWithPolicy(thing, placeholderOrId, connectionName)
                .withDittoHeaders(headers)
                .withExpectedStatus(expectedStatus)
                .fire();
    }

    void createNewThingWithPolicyRef(final Thing thing, final String placeholderOrId, final String connectionName,
            final int expectedStatus) {
        new CreateNewThingWithPolicy(thing, placeholderOrId, connectionName)
                .withExpectedStatus(expectedStatus)
                .fire();
    }

    protected final ThingId sendCreateThingAndEnsureResponseIsSentBack(final String connectionName,
            final Function<ThingBuilder.FromScratch, Thing> thingModifier) {

        final CreateThing createThing = buildCreateThing(connectionName, thingModifier);
        final ThingId thingEntityId = createThing.getEntityId();
        final CommandResponse<?> response = sendCommandAndEnsureResponseIsSentBack(connectionName, createThing);
        assertThat(response).isInstanceOf(CreateThingResponse.class);
        assertThat(response.getHttpStatus()).isEqualTo(HttpStatus.CREATED);
        assertThat(((CreateThingResponse) response).getThingCreated()
                .flatMap(Thing::getEntityId))
                .contains(thingEntityId);
        return thingEntityId;
    }

    class CreateNewThingWithPolicy {

        final Thing thing;
        @Nullable final Policy policy;
        @Nullable final String placeholderOrId;
        final String connectionName;
        DittoHeaders headers = DittoHeaders.empty();
        int expectedStatus = 0;

        private CreateNewThingWithPolicy(final Thing thing, @Nullable final String placeholderOrId,
                final String connectionName) {
            this.thing = thing;
            this.policy = null;
            this.placeholderOrId = placeholderOrId;
            this.connectionName = connectionName;
        }

        private CreateNewThingWithPolicy(final Thing thing, @Nullable final Policy policy,
                final String connectionName) {
            this.thing = thing;
            this.policy = policy;
            this.placeholderOrId = null;
            this.connectionName = connectionName;
        }

        CreateNewThingWithPolicy withDittoHeaders(final DittoHeaders headers) {
            this.headers = headers;
            return this;
        }

        CreateNewThingWithPolicy withExpectedStatus(final int status) {
            this.expectedStatus = status;
            return this;
        }

        void fire() {
            final JsonSchemaVersion schemaVersion = JsonSchemaVersion.V_2;
            final String correlationId = createNewCorrelationId();
            final DittoHeaders dittoHeaders = DittoHeaders.of(headers).toBuilder()
                    .schemaVersion(schemaVersion)
                    .correlationId(correlationId)
                    .build();
            final JsonObject initialPolicyJson = policy == null ? null : policy.toJson();
            final CreateThing createThing = CreateThing.of(thing, initialPolicyJson, placeholderOrId, dittoHeaders);

            sendSignal(connectionName, createThing);
            final C consumer = initResponseConsumer(connectionName, correlationId);

            final M message = consumeResponse(correlationId, consumer);
            assertThat(message).isNotNull();
            assertThat(getCorrelationId(message)).isEqualTo(correlationId);
            if (expectedStatus > 0) {
                final JsonObject jsonMessage = getConnectivityWorker().jsonifiableAdaptableFrom(message).toJson();
                final Optional<Integer> statusOptional = jsonMessage.getValue(CommandResponse.JsonFields.STATUS);
                assertThat(statusOptional).contains(expectedStatus);
            }
        }

    }

    void createNewThingWithPolicy(final ThingId thingId, final String featureId, final String creatingConnectionName,
            final String... furtherAuthorizedConnections) {
        final Policy policy = createNewPolicy(thingId, featureId, creatingConnectionName, furtherAuthorizedConnections);

        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setFeature(featureId, FeatureProperties.newBuilder().build())
                .build();

        createNewThing(thing, policy, creatingConnectionName);
    }

    Policy createNewPolicy(final EntityId entityId, final String featureId, final String connectionName,
            final String... connectionNames) {
        final List<Subject> subjects = Arrays.stream(connectionNames)
                .map(this::connectionSubject)
                .collect(Collectors.toList());

        final Subject connectivitySubject = connectionSubject(connectionName);
        subjects.add(connectivitySubject);

        subjects.add(testingContextWithRandomNs.getOAuthClient().getDefaultSubject());

        return Policy.newBuilder(PolicyId.of(entityId))
                .forLabel("OWNER")
                .setSubjects(Subjects.newInstance(subjects))
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/features/" + featureId), READ, WRITE)
                .build();
    }

}
