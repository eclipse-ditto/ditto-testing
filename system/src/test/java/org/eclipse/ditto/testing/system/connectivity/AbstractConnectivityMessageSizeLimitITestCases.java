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

import java.util.Arrays;
import java.util.UUID;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.json.Jsonifiable;
import org.eclipse.ditto.base.model.signals.commands.CommandResponse;
import org.eclipse.ditto.base.model.signals.commands.ErrorResponse;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.testing.common.categories.RequireSource;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.ThingErrorResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThingResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.ModifyAttribute;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public abstract class AbstractConnectivityMessageSizeLimitITestCases<C, M>
        extends AbstractConnectivityPlaceholdersITestCases<C, M> {

    protected AbstractConnectivityMessageSizeLimitITestCases(final ConnectivityFactory cf) {
        super(cf);
    }

    @Test
    @Category(RequireSource.class)
    @Connections(ConnectionCategory.CONNECTION1)
    public void sendTooLargeCreateThingAndExpectError() {
        // Given
        final ThingId thingId = generateThingId();

        final JsonObject largeAttributes = JsonObject.newBuilder()
                .set("a", "a".repeat(DEFAULT_MAX_THING_SIZE))
                .build();

        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setAttributes(largeAttributes)
                .build();

        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(connectionSubject(cf.connectionName1))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();

        sendCreateThingAndExpectError(thing, policy, cf.connectionName1, HttpStatus.REQUEST_ENTITY_TOO_LARGE);
    }

    @Test
    @Category(RequireSource.class)
    @Connections(ConnectionCategory.CONNECTION1)
    public void sendCreateThingWithTooLargeHeaderFieldAndExpectError() {
        // Given
        final ThingId thingId = generateThingId();

        final char[] chars = new char[8192];
        Arrays.fill(chars, 'x');
        final String largeString = new String(chars);

        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .build();

        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(connectionSubject(cf.connectionName1))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();

        sendCreateThingAndExpectError(thing, policy, cf.connectionName1, HttpStatus.REQUEST_HEADER_FIELDS_TOO_LARGE,
                entry("customHeaderField", largeString));
    }

    @Test
    @Category(RequireSource.class)
    @Connections(ConnectionCategory.CONNECTION1)
    public void sendCreateThingThenMakeItTooLargeAndExpectError() {
        // Given
        final ThingId thingId = generateThingId();
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(connectionSubject(cf.connectionName1))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();
        final int policySizeInBytes = policy.toJson().toString().getBytes().length;
        final JsonObject largeAttributes = JsonObject.newBuilder()
                .set("a", "a".repeat(DEFAULT_MAX_THING_SIZE - policySizeInBytes))
                .build();
        final Thing thing = Thing.newBuilder()
                .setId(thingId)
                .setAttributes(largeAttributes)
                .build();
        final String correlationId = UUID.randomUUID().toString();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .correlationId(correlationId)
                .build();
        final CreateThing createThing = CreateThing.of(thing, policy.toJson(), dittoHeaders);

        // When
        sendSignal(cf.connectionName1, createThing);

        // Then
        final C consumer = initResponseConsumer(cf.connectionName1, correlationId);
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
        assertThat(response).isInstanceOf(CreateThingResponse.class);
        assertThat(((CommandResponse) response).getHttpStatus()).isEqualTo(HttpStatus.CREATED);

        // update attribute "b" which should exceed the max thing size
        final JsonObject largeAttributesB = JsonObject.newBuilder()
                .set("a", "a".repeat(policySizeInBytes))
                .build();

        final String correlationId2 = UUID.randomUUID().toString();
        final DittoHeaders dittoHeaders2 = DittoHeaders.newBuilder()
                .schemaVersion(JsonSchemaVersion.V_2)
                .correlationId(correlationId2)
                .build();

        final ModifyAttribute modifyAttribute =
                ModifyAttribute.of(thingId, JsonPointer.of("b"), largeAttributesB, dittoHeaders2);

        // When
        sendSignal(cf.connectionName1, modifyAttribute);

        // Then
        final C consumer2 = initResponseConsumer(cf.connectionName1, correlationId2);
        final M message2 = consumeResponse(correlationId2, consumer2);
        assertThat(message2).isNotNull();
        assertThat(getCorrelationId(message2)).isEqualTo(correlationId2);
        checkHeaders(message2);

        final JsonifiableAdaptable jsonifiableAdaptable2 = jsonifiableAdaptableFrom(message2);
        final Adaptable responseAdaptable2 =
                ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable2).build();
        final Jsonifiable<JsonObject> response2 = PROTOCOL_ADAPTER.fromAdaptable(responseAdaptable2);

        assertThat(response2).isNotNull();
        assertThat(response2).isInstanceOf(ErrorResponse.class);
        assertThat(response2).isInstanceOf(ThingErrorResponse.class);
        assertThat(((ThingErrorResponse) response2).getHttpStatus()).isEqualTo(HttpStatus.REQUEST_ENTITY_TOO_LARGE);
    }

}
