/*
 * Copyright (c) 2025 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 */
package org.eclipse.ditto.testing.system.connectivity;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.eclipse.ditto.base.model.auth.AuthorizationModelFactory;
import org.eclipse.ditto.base.model.auth.DittoAuthorizationContextType;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.HeaderMapping;
import org.eclipse.ditto.connectivity.model.Source;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.testing.common.categories.RequireSource;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.ThingErrorResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThingResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThingResponse;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.response.Response;

/**
 * System tests for the response diversion functionality.
 * Tests the ability to divert command responses to alternative connection targets.
 */
public abstract class AbstractConnectivityResponseDiversionITestCases<C, M>
        extends AbstractConnectivityITestCases<C, M> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConnectivityResponseDiversionITestCases.class);
    protected static final String DIVERSION_SOURCE = "division-source";
    protected static final String DIVERSION_TARGET = "division-target";


    private static final String NO_OP = "no-op-mod";

    static {
        addMod(DIVERSION_SOURCE, connection -> {
            final Source firstSource = connection.getSources().get(0);
            List<Source> sources = new ArrayList<>();
            final Map<String, String> mapping = new HashMap<>(firstSource.getHeaderMapping().getMapping());
            mapping.put(DittoHeaderDefinition.DIVERT_EXPECTED_RESPONSE_TYPES.getKey(), "response,error");
            sources.add(ConnectivityModelFactory.newSourceBuilder(firstSource)
                    .replyTargetEnabled(true)
                    .headerMapping(ConnectivityModelFactory.newHeaderMapping(mapping))
                    .build());
            if (connection.getSources().size() > 1){
                final Source secondSource = connection.getSources().get(connection.getSources().size() - 1);;
                final Source source = ConnectivityModelFactory.newSourceBuilder(secondSource)
                        .replyTargetEnabled(true)
                        .authorizationContext(AuthorizationModelFactory.newAuthContext(
                                DittoAuthorizationContextType.PRE_AUTHENTICATED_CONNECTION,
                                AuthorizationModelFactory.newAuthSubject("unauthorized-subject")))
                        .headerMapping(ConnectivityModelFactory.newHeaderMapping(mapping))
                        .build();
                sources.add(source);
            }

            final Map<String, String> specificConfig = new HashMap<>(connection.getSpecificConfig());
            specificConfig.remove(DittoHeaderDefinition.REPLY_TO.getKey());
            specificConfig.put("is-diversion-source", "true");
            return connection.toBuilder()
                    .name(DIVERSION_SOURCE)
                    .setTargets(List.of())
                    .setSources(sources)
                    .clientCount(1)
                    .specificConfig(specificConfig)
                    .build();
        });
        addMod(DIVERSION_TARGET, connection -> {
            final Map<String, String> specificConfig = new HashMap<>(connection.getSpecificConfig());
            specificConfig.put("is-diversion-target", "true");
            return connection.toBuilder()
                    .name(DIVERSION_TARGET)
                    .setSources(List.of())
                    .setTargets(List.of(removeTopicsFrom(connection.getTargets().get(0))))
                    .specificConfig(specificConfig)
                    .clientCount(1)
                    .build();
        });
        addMod(NO_OP, connection -> connection);
    }

    protected AbstractConnectivityResponseDiversionITestCases(final ConnectivityFactory cf) {
        super(cf);
    }

    @Test
    @Category(RequireSource.class)
    @UseConnection(category = ConnectionCategory.CONNECTION1, mod = DIVERSION_SOURCE)
    @UseConnection(category = ConnectionCategory.CONNECTION2, mod = DIVERSION_TARGET)
    public void divertResponseBasedOnConnectionHeaderMapping() {
        final Connection targetConnection = getConnectionExistingByNameForUser(DIVERSION_TARGET, getUsername());
        final String targetConnectionId = targetConnection.getId().toString();
        final Connection sourceConnection = getConnectionExistingByNameForUser(DIVERSION_SOURCE, getUsername());
        prepareConnectionsForDiversion(sourceConnection, targetConnectionId, targetConnection, true);

        // Given
        final String sendingConnectionName = cf.connectionName1;
        final String divertedConnectionName = cf.connectionName2;
        final CreateThing createThing = newCreateThing(sendingConnectionName, false);
        sendSignal(sendingConnectionName, createThing);

        final String correlationId = UUID.randomUUID().toString();
        // When: Send command with divert-response-to-connection header
        final RetrieveThing retrieveThing = RetrieveThing.of(createThing.getEntityId(),
                DittoHeaders.newBuilder()
                        .correlationId(correlationId)
                        .schemaVersion(JsonSchemaVersion.V_2)
                        .build());

        final String divertAddress = getTargetAddress(targetConnection);
        final C responseConsumer = initTargetsConsumer(divertedConnectionName, divertAddress);
        sendSignal(sendingConnectionName, retrieveThing);

        // Then: Response should be received at diverted address
        final M divertedMessage = consumeFromTarget(divertedConnectionName, responseConsumer);
        assertThat(divertedMessage).isNotNull();

        final Signal<?> signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(divertedMessage));
        assertThat(signal).isInstanceOf(RetrieveThingResponse.class);
        assertThat(getCorrelationId(divertedMessage)).isEqualTo(correlationId);
    }

    private String getUsername() {
        return testingContextWithRandomNs.getSolution().getUsername();
    }

    @Test
    @UseConnection(category = ConnectionCategory.CONNECTION1, mod = DIVERSION_SOURCE)
    @UseConnection(category = ConnectionCategory.CONNECTION2, mod = DIVERSION_TARGET)
    public void divertResponseBasedOnHeaderInTheMessageItself() {
        final Connection targetConnection = getConnectionExistingByNameForUser(DIVERSION_TARGET,
                getUsername());
        final Connection sourceConnection = getConnectionExistingByNameForUser(DIVERSION_SOURCE,
                getUsername());
        final String targetConnectionId = targetConnection.getId().toString();
        prepareConnectionsForDiversion(sourceConnection, targetConnectionId, targetConnection, false);

        // Given
        final String sendingConnectionName = cf.connectionName1;
        final String divertedConnectionName = cf.connectionName2;
        final CreateThing createThing = newCreateThing(sendingConnectionName, false);
        sendSignal(sendingConnectionName, createThing);

        final String correlationId = UUID.randomUUID().toString();
        final String divertAddress = getTargetAddress(targetConnection);
        // When: Send command with divert-response-to-connection header
        final RetrieveThing retrieveThing = RetrieveThing.of(createThing.getEntityId(),
                DittoHeaders.newBuilder()
                        .correlationId(correlationId)
                        .schemaVersion(JsonSchemaVersion.V_2)
                        .putHeader(DittoHeaderDefinition.DIVERT_RESPONSE_TO_CONNECTION.getKey(), targetConnectionId)
                        .build());
        final C responseConsumer = initTargetsConsumer(divertedConnectionName, divertAddress);
        sendSignal(sendingConnectionName, retrieveThing);

        // Then: Response should be received at diverted address
        final M divertedMessage = consumeFromTarget(correlationId, responseConsumer);
        assertThat(divertedMessage).isNotNull();

        final Signal<?> signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(divertedMessage));
        assertThat(signal).isInstanceOf(RetrieveThingResponse.class);
        assertThat(getCorrelationId(divertedMessage)).isEqualTo(correlationId);
    }

    @Test
    @UseConnection(category = ConnectionCategory.CONNECTION1, mod = DIVERSION_SOURCE)
    @UseConnection(category = ConnectionCategory.CONNECTION2, mod = DIVERSION_TARGET)
    public void doNotPublishDivertedResponseIfNotAuthorized() {
        final Connection targetConnection = getConnectionExistingByNameForUser(DIVERSION_TARGET,
                getUsername());
        final Connection sourceConnection = getConnectionExistingByNameForUser(DIVERSION_SOURCE,
                getUsername());
        final String targetConnectionId = targetConnection.getId().toString();

        // Given
        final String sendingConnectionName = cf.connectionName1;
        final String divertedConnectionName = cf.connectionName2;
        final CreateThing createThing = newCreateThing(sendingConnectionName, false);
        sendSignal(sendingConnectionName, createThing);

        addDiversionHeaderMappingToConnectionSource(sourceConnection, targetConnectionId);

        final String correlationId = UUID.randomUUID().toString();
        final String divertAddress = getTargetAddress(targetConnection);
        final RetrieveThing retrieveThing = RetrieveThing.of(createThing.getEntityId(),
                DittoHeaders.newBuilder()
                        .correlationId(correlationId)
                        .schemaVersion(JsonSchemaVersion.V_2)
                        .build());
        final C responseConsumer = initTargetsConsumer(divertedConnectionName, divertAddress);
        sendSignal(sendingConnectionName, retrieveThing);

        // Then: No response should be diverted address
        final M divertedMessage = consumeFromTarget(correlationId, responseConsumer);
        assertThat(divertedMessage).isNull();
    }

    @Test
    @Category(RequireSource.class)
    @UseConnection(category = ConnectionCategory.CONNECTION1, mod = DIVERSION_TARGET)
    @UseConnection(category = ConnectionCategory.CONNECTION2, mod = NO_OP)
    @UseConnection(category = ConnectionCategory.CONNECTION_WITH_2_SOURCES, mod = DIVERSION_SOURCE)
    public void testErrorResponseDiversion() {

        final Connection targetConnection = getConnectionExistingByNameForUser(DIVERSION_TARGET, getUsername());
        final String targetConnectionId = targetConnection.getId().toString();
        final Connection sourceConnection = getConnectionExistingByNameForUser(DIVERSION_SOURCE, getUsername());
        prepareConnectionsForDiversion(sourceConnection, targetConnectionId, targetConnection, true);

        final CreateThing createThing = newCreateThing(cf.connectionName2, true);

        final String correlationId = createThing.getDittoHeaders().getCorrelationId().get();
        final C responseConsumer = initResponseConsumer(cf.connectionName2, correlationId);
        sendSignal(cf.connectionName2, createThing);
        final M createResponse = consumeResponse(correlationId, responseConsumer);
        assertThat(createResponse).isNotNull();

        final Signal<?> signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(createResponse));
        assertThat(signal).isInstanceOf(CreateThingResponse.class);


        // Send command that will fail
        final RetrieveThing retrieveThing = RetrieveThing.of(createThing.getEntityId(),
                DittoHeaders.newBuilder()
                        .correlationId(createNewCorrelationId())
                        .schemaVersion(JsonSchemaVersion.V_2)
                        .build());

        final String divertAddress = getTargetAddress(targetConnection);
        final C targetsConsumer = initTargetsConsumer(cf.connectionName1, divertAddress);
        sendSignal(cf.connectionWith2Sources + ConnectionModelFactory.SOURCE2_SUFFIX, retrieveThing);

        // Then: Error response should be received at diverted address
        final M divertedMessage = consumeFromTarget(cf.connectionName1, targetsConsumer);
        assertThat(divertedMessage).isNotNull();

        final Signal<?> errorResponseSignal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(divertedMessage));
        assertThat(errorResponseSignal).isInstanceOf(ThingErrorResponse.class);
    }

    private void prepareConnectionsForDiversion(final Connection sourceConnection, final String targetConnectionId,
            final Connection targetConnection, final boolean addDiversionHeaderMapping) {
        if (addDiversionHeaderMapping) {
            addDiversionHeaderMappingToConnectionSource(sourceConnection, targetConnectionId);
        }
        addAuthorizedSourcesToTargetConnection(targetConnection, sourceConnection);
    }

    private static Target removeTopicsFrom(final Target target) {
        return ConnectivityModelFactory.newTargetBuilder(target)
                .topics(Set.of())
                .build();
    }

    private void addDiversionHeaderMappingToConnectionSource(final Connection sourceConnection,
            final String targetConnectionId) {
        final List<Source> newSources = sourceConnection.getSources().stream().map(source -> {
            final Map<String, String> mapping = new HashMap<>(source.getHeaderMapping().getMapping());
            mapping.put(DittoHeaderDefinition.DIVERT_RESPONSE_TO_CONNECTION.getKey(), targetConnectionId);
            final HeaderMapping newHeaderMapping = ConnectivityModelFactory.newHeaderMapping(mapping);
            return ConnectivityModelFactory.newSourceBuilder(source)
                    .headerMapping(newHeaderMapping)
                    .build();
        }).toList();

        final Connection connection = sourceConnection.toBuilder()
                .setSources(newSources).build();
        final Response response = modifyConnection(connection);
        LOGGER.info("connection modified. Response: {}", response.statusCode());
    }

    private void addAuthorizedSourcesToTargetConnection(final Connection targetConnection,
            final Connection sourceConnection) {
        final HashMap<String, String> specificConfig = new HashMap<>(targetConnection.getSpecificConfig());
        specificConfig.put("authorized-connections-as-sources", sourceConnection.getId().toString());
        final Connection modifiedTargetConnection = targetConnection.toBuilder()
                .specificConfig(specificConfig)
                .build();
        final Response response = modifyConnection(modifiedTargetConnection);
        LOGGER.info("Target connection modified. Response: {}", response.statusCode());
        assertThat(response.statusCode()).isGreaterThanOrEqualTo(200);
        assertThat(response.statusCode()).isLessThan(300);
    }

    // Helper methods
    protected String getTargetAddress(final Connection targetConnection) {
        return targetConnection.getTargets().get(0).getAddress();
    }
}