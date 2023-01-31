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

import java.util.UUID;

import javax.annotation.Nullable;

import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Integration test cases for Connectivity tests which only support targets (e.g. HTTP).
 * Implemented by concrete Connectivity types.
 *
 * @param <C> the consumer
 * @param <M> the consumer message
 */
public abstract class AbstractTargetOnlyTestCases<C, M> extends AbstractConnectivityITestCases<C, M> {

    private static ConnectivityTestWebsocketClient websocketClient;

    protected AbstractTargetOnlyTestCases(final ConnectivityFactory cf) {
        super(cf);
    }

    @Override
    protected boolean connectionTypeSupportsSource() {
        return false;
    }

    @BeforeClass
    public static void setUpWebsocketClient() {
        websocketClient = ConnectivityTestWebsocketClient.newInstance(thingsWsUrl(TestConstants.API_V_2),
                SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient().getAccessToken());
        websocketClient.connect("kafka-source-replacement-websocket-" + UUID.randomUUID());
    }

    @AfterClass
    public static void closeWebsocketClient() {
        if (websocketClient != null) {
            websocketClient.disconnect();
            websocketClient = null;
        }
    }

    @Override
    protected void sendSignal(@Nullable final String connectionName, final Signal<?> signal) {
        websocketClient.send(signal);
    }

    // override newCreateThing to not include sending connection name in the permissions because
    // the command will be sent over websocket.
    @Override
    protected CreateThing newCreateThing(final String sendingConnectionName, final String receivingConnectionName) {
        return newTargetOnlyCreateThing(receivingConnectionName, receivingConnectionName);
    }

    protected CreateThing newTargetOnlyCreateThing(final String receivingConnectionName,
            final String receivingConnectionName2) {
        final ThingId thingId = generateThingId();
        final PolicyId policyId = PolicyId.of(thingId);
        final Policy policy = policyWithAccessFor(policyId, receivingConnectionName, receivingConnectionName2);
        final Thing thing = Thing.newBuilder().setId(thingId).build();
        final JsonSchemaVersion schemaVersion = JsonSchemaVersion.V_2;
        final String correlationId = createNewCorrelationId();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .schemaVersion(schemaVersion)
                .correlationId(correlationId)
                .build();

        return CreateThing.of(thing, policy.toJson(), dittoHeaders);
    }

    protected CreateThing newTargetOnlyCreateThingWithFeature(final String receivingConnectionName,
            final String receivingConnectionName2, final Feature feature) {
        final ThingId thingId = generateThingId();
        final PolicyId policyId = PolicyId.of(thingId);
        final Policy policy = policyWithAccessFor(policyId, receivingConnectionName, receivingConnectionName2);
        final Thing thing = Thing.newBuilder().setId(thingId)
                .setFeature(feature)
                .build();
        final JsonSchemaVersion schemaVersion = JsonSchemaVersion.V_2;
        final String correlationId = createNewCorrelationId();
        final DittoHeaders dittoHeaders = DittoHeaders.newBuilder()
                .schemaVersion(schemaVersion)
                .correlationId(correlationId)
                .build();

        return CreateThing.of(thing, policy.toJson(), dittoHeaders);
    }

}
