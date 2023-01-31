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
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION1;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION2;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION_WITH_AUTH_PLACEHOLDER_ON_HEADER_ID;
import static org.eclipse.ditto.testing.system.connectivity.ConnectivityConstants.HEADER_ID;

import java.util.Map;
import java.util.UUID;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.json.Jsonifiable;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.base.model.signals.commands.ErrorResponse;
import org.eclipse.ditto.json.JsonKey;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.json.assertions.DittoJsonAssertions;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.testing.common.categories.RequireProtocolHeaders;
import org.eclipse.ditto.testing.common.categories.RequireSource;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttribute;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyFeatureProperty;
import org.eclipse.ditto.things.model.signals.events.AttributeCreated;
import org.eclipse.ditto.things.model.signals.events.FeaturePropertyCreated;
import org.eclipse.ditto.things.model.signals.events.ThingCreated;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public abstract class AbstractConnectivityPlaceholdersITestCases<C, M>
        extends AbstractConnectivityLiveMessagesITestCases<C, M> {

    protected AbstractConnectivityPlaceholdersITestCases(final ConnectivityFactory cf) {
        super(cf);
    }

    @Test
    @Connections({CONNECTION2, CONNECTION_WITH_AUTH_PLACEHOLDER_ON_HEADER_ID})
    public void sendSingleCommandAndEnsurePlaceholderSubstitution() {

        final String placeholderValue = UUID.randomUUID().toString();
        // Given
        final ThingId thingId = generateThingId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(cf.connectionNameWithAuthPlaceholderOnHEADER_ID))
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
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(correlationId)
                .putHeader(HEADER_ID, placeholderValue)
                .responseRequired(false)
                .build();
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), dittoHeaders);

        // When
        final C eventConsumer = initTargetsConsumer(cf.connectionName2);
        sendSignal(cf.connectionNameWithAuthPlaceholderOnHEADER_ID, createThing);

        // Then
        final String expectedSource =
                getSendSingleCommandAndEnsurePlaceholderSubstitutionExpectedSource(placeholderValue);
        consumeAndAssert(cf.connectionName2, thingId, correlationId, eventConsumer, ThingCreated.class,
                tc -> {
                    assertThat(tc.getRevision()).isEqualTo(1L);
                    if (connectionTypeSupportsSource()) {
                        // do not check "source" header for target-only connections:
                        // the event is deserialized without taking external protocol headers into consideration
                        // and cannot contain the mapped header request:subjectId.
                        assertThat(tc.getDittoHeaders().get("source"))
                                .describedAs("source <- request:subjectId header")
                                .contains(expectedSource);
                    }
                });
    }

    protected String getSendSingleCommandAndEnsurePlaceholderSubstitutionExpectedSource(final String placeholderValue) {
        return String.format("integration:%s:%s", SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution().getUsername(),
                placeholderValue);
    }

    @Test
    @Connections({CONNECTION_WITH_AUTH_PLACEHOLDER_ON_HEADER_ID, CONNECTION1})
    public void targetAddressPlaceholderSubstitution() {

        // Given
        final ThingId thingId = generateThingId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(cf.connectionName1))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .forLabel("RESTRICTED")
                .setSubject(connectionSubject(cf.connectionNameWithAuthPlaceholderOnHEADER_ID))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ)
                .build();
        final String correlationId = createNewCorrelationId();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .correlationId(correlationId)
                .responseRequired(false)
                .build();
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), dittoHeaders);


        // When
        final C eventConsumer =
                initTargetsConsumer(cf.connectionNameWithAuthPlaceholderOnHEADER_ID,
                        targetAddressForTargetPlaceHolderSubstitution());
        sendSignal(cf.connectionName1, createThing);

        // Then
        consumeAndAssert(cf.connectionNameWithAuthPlaceholderOnHEADER_ID, thingId, correlationId, eventConsumer,
                ThingCreated.class,
                tc -> {
                    assertThat(tc.getRevision()).isEqualTo(1L);
                    assertThat(tc.getThing().getEntityId()).isEqualTo(thing.getEntityId());
                });
    }

    @Test
    @Category({RequireSource.class, RequireProtocolHeaders.class})
    @Connections({CONNECTION1, CONNECTION_WITH_AUTH_PLACEHOLDER_ON_HEADER_ID})
    public void ensureFeatureIdPlaceholderSubstitution() {
        // Given
        final C eventConsumer =
                initTargetsConsumer(cf.connectionNameWithAuthPlaceholderOnHEADER_ID,
                        targetAddressForTargetPlaceHolderSubstitution());

        final ThingId thingId = generateThingId();
        final String featureId = "FluxCapacitor";
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setFeature(featureId)
                .build();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(cf.connectionName1))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .forLabel("RESTRICTED")
                .setSubject(connectionSubject(cf.connectionNameWithAuthPlaceholderOnHEADER_ID))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ)
                .build();
        final String createThingCorrelationId = createNewCorrelationId();
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), DittoHeaders.newBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .correlationId(createThingCorrelationId)
                .responseRequired(false)
                .build());
        sendSignal(cf.connectionName1, createThing);

        // Consume ThingCreated
        final M eventMessage1 = consumeFromTarget(cf.connectionNameWithAuthPlaceholderOnHEADER_ID, eventConsumer);
        assertThat(eventMessage1).withFailMessage("Did not receive an event.").isNotNull();
        final Jsonifiable<JsonObject> event1 = getJsonifiableEvent(eventMessage1);
        final ThingCreated tc = thingEventForJson(event1, ThingCreated.class, createThingCorrelationId, thingId);
        assertThat(tc.getRevision()).isEqualTo(1L);

        // When
        final String modifyFeaturePropertyCorrelationId = createNewCorrelationId();
        final ModifyFeatureProperty modifyFeatureProperty = ModifyFeatureProperty.of(thingId, featureId,
                JsonPointer.of("voltage"), JsonValue.of(42), DittoHeaders.newBuilder()
                        .schemaVersion(JsonSchemaVersion.V_2)
                        .correlationId(modifyFeaturePropertyCorrelationId)
                        .responseRequired(false)
                        .build());
        sendSignal(cf.connectionName1, modifyFeatureProperty);

        // Then
        // Consume FeaturePropertyCreated
        final M eventMessage2 = consumeFromTarget(cf.connectionNameWithAuthPlaceholderOnHEADER_ID, eventConsumer);
        assertThat(eventMessage2).withFailMessage("Did not receive an event.").isNotNull();
        final Jsonifiable<JsonObject> event2 = getJsonifiableEvent(eventMessage2);
        final FeaturePropertyCreated fpc =
                thingEventForJson(event2, FeaturePropertyCreated.class, modifyFeaturePropertyCorrelationId, thingId);
        assertThat(fpc.getRevision()).isEqualTo(2L);
        final Map<String, String> receivedProtocolHeaders = checkHeaders(eventMessage2);
        assertThat(receivedProtocolHeaders).containsEntry("feature-id", featureId);
    }

    private Jsonifiable<JsonObject> getJsonifiableEvent(final M eventMessage) {
        final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(eventMessage);
        final Adaptable eventAdaptable = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
        return PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable);
    }

    @Test
    @Category(RequireSource.class)
    @Connections({ConnectionCategory.CONNECTION2, CONNECTION_WITH_AUTH_PLACEHOLDER_ON_HEADER_ID})
    public void ensureSourceAuthenticationPlaceholderSubstitution() {
        final String additionalAuthSubject = UUID.randomUUID().toString();
        // Given
        final ThingId thingId = generateThingId();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(additionalAuthSubject))
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
        final String forbiddenCorrelationId = createNewCorrelationId();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .correlationId(correlationId)
                .putHeader(HEADER_ID, additionalAuthSubject)
                .responseRequired(false)
                .build();

        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), dittoHeaders);

        final JsonPointer modAttrPointer = JsonPointer.of("modAttr");
        final JsonValue modAttrValue = JsonValue.of(42);

        final ModifyAttribute allowedModifyAttribute =
                ModifyAttribute.of(thingId, modAttrPointer, modAttrValue, createDittoHeaders(correlationId)
                        .toBuilder()
                        .putHeader(HEADER_ID, additionalAuthSubject)
                        .build());
        final ModifyAttribute forbiddenModifyAttribute =
                ModifyAttribute.of(thingId, modAttrPointer, modAttrValue, createDittoHeaders(forbiddenCorrelationId)
                        .toBuilder()
                        .putHeader(HEADER_ID, "this-is-absolutely-wrong-dude")
                        .build());

        final C eventConsumer = initTargetsConsumer(cf.connectionName2);
        final C responseConsumer =
                initResponseConsumer(cf.connectionNameWithAuthPlaceholderOnHEADER_ID, forbiddenCorrelationId);

        sendSignal(cf.connectionNameWithAuthPlaceholderOnHEADER_ID, createThing);
        consumeAndAssert(cf.connectionName2, thingId, correlationId, eventConsumer, ThingCreated.class,
                tc -> assertThat(tc.getRevision()).isEqualTo(1L));

        // When
        sendSignal(cf.connectionNameWithAuthPlaceholderOnHEADER_ID, allowedModifyAttribute);
        sendSignal(cf.connectionNameWithAuthPlaceholderOnHEADER_ID, forbiddenModifyAttribute);

        // Then
        // verify allowedModifyAttribute is received
        consumeAndAssert(cf.connectionName2, thingId, correlationId, eventConsumer, AttributeCreated.class,
                ac -> {
                    assertThat(ac.getRevision()).isEqualTo(2L);
                    assertThat((Iterable<? extends JsonKey>) ac.getAttributePointer()).isEqualTo(modAttrPointer);
                    DittoJsonAssertions.assertThat(ac.getAttributeValue()).isEqualTo(modAttrValue);
                });

        // verify forbiddenModifyAttribute was rejected
        final M response = consumeResponse(forbiddenCorrelationId, responseConsumer);
        final Signal<?> signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(response));
        assertThat(signal).isInstanceOf(ErrorResponse.class);
        final ErrorResponse errorResponse = (ErrorResponse) signal;
        assertThat(errorResponse.getDittoRuntimeException().getHttpStatus()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(errorResponse.getDittoRuntimeException().getErrorCode()).isEqualTo("things:attribute.notmodifiable");
    }

}
