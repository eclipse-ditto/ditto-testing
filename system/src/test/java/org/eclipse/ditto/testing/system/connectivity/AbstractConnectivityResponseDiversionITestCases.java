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
import static org.eclipse.ditto.testing.system.connectivity.ConnectionModelFactory.SOURCE1_SUFFIX;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.eclipse.ditto.base.model.auth.AuthorizationModelFactory;
import org.eclipse.ditto.base.model.auth.DittoAuthorizationContextType;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.signals.Signal;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionBuilder;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.HeaderMapping;
import org.eclipse.ditto.connectivity.model.Source;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonObjectBuilder;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.testing.common.PiggyBackCommander;
import org.eclipse.ditto.testing.common.categories.RequireSource;
import org.eclipse.ditto.testing.common.matcher.PostMatcher;
import org.eclipse.ditto.testing.common.matcher.StatusCodeSuccessfulMatcher;
import org.eclipse.ditto.testing.common.piggyback.PiggybackRequest;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.commands.ThingErrorResponse;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThing;
import org.eclipse.ditto.things.model.signals.commands.modify.CreateThingResponse;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThing;
import org.eclipse.ditto.things.model.signals.commands.query.RetrieveThingResponse;
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
            final Source firstSource = connection.getSources().getFirst();
            List<Source> sources = new ArrayList<>();
            final Map<String, String> mapping = new HashMap<>(firstSource.getHeaderMapping().getMapping());
            mapping.put(DittoHeaderDefinition.DIVERT_EXPECTED_RESPONSE_TYPES.getKey(), "response,error");
            sources.add(ConnectivityModelFactory.newSourceBuilder(firstSource)
                    .replyTargetEnabled(true)
                    .headerMapping(ConnectivityModelFactory.newHeaderMapping(mapping))
                    .build());
            if (connection.getSources().size() > 1) {
                final Source secondSource = connection.getSources().getLast();
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
                    .setTargets(List.of(removeTopicsFrom(connection.getTargets().getFirst())))
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
    public void divertResponseBasedOnConnectionHeaderMapping()
            throws ExecutionException, InterruptedException, TimeoutException {
        final String sourceConnectionName = cf.connectionName1;
        final Connection sourceConnection = getConnectionExistingByNameForUser(DIVERSION_SOURCE, getUsername());
        final String targetConnectionName = cf.connectionName2;
        final Connection targetConnection = getConnectionExistingByNameForUser(DIVERSION_TARGET, getUsername());

        final ThingId thingId = sendCreateThingAndEnsureResponseIsSentBack(cf.connectionName1,
                ThingBuilder.FromScratch::build);

        prepareConnectionsForDiversion(sourceConnection, true, false, targetConnection, true);

        // Given
        final String correlationId = UUID.randomUUID().toString();
        // When: Send command with divert-response-to-connection header
        final RetrieveThing retrieveThing = RetrieveThing.of(thingId,
                DittoHeaders.newBuilder()
                        .correlationId(correlationId)
                        .schemaVersion(JsonSchemaVersion.V_2)
                        .build());

        final String divertAddress = getTargetAddress(targetConnection);
        final C responseConsumer = initTargetsConsumer(targetConnectionName, divertAddress);
        sendSignal(sourceConnectionName, retrieveThing);

        // Then: Response should be received at diverted address
        final M divertedMessage = consumeFromTarget(targetConnectionName, responseConsumer);
        assertThat(divertedMessage).isNotNull();

        final Signal<?> signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(divertedMessage));
        assertThat(signal).isInstanceOf(RetrieveThingResponse.class);
        assertThat(getCorrelationId(divertedMessage)).isEqualTo(correlationId);
    }


    @Test
    @Category(RequireSource.class)
    @UseConnection(category = ConnectionCategory.CONNECTION1, mod = DIVERSION_SOURCE)
    @UseConnection(category = ConnectionCategory.CONNECTION2, mod = DIVERSION_TARGET)
    public void divertAndPreserveOriginalResponseInSourceConnection()
            throws ExecutionException, InterruptedException, TimeoutException {
        final String sourceConnectionName = cf.connectionName1;
        final String targetConnectionName = cf.connectionName2;
        final ThingId thingId = sendCreateThingAndEnsureResponseIsSentBack(sourceConnectionName,
                ThingBuilder.FromScratch::build);

        final Connection sourceConnection = getConnectionExistingByNameForUser(DIVERSION_SOURCE, getUsername());
        final Connection targetConnection = getConnectionExistingByNameForUser(DIVERSION_TARGET, getUsername());
        prepareConnectionsForDiversion(sourceConnection, true, true, targetConnection, true);

        // Given

        final String correlationId = UUID.randomUUID().toString();
        // When: Send command with divert-response-to-connection header
        final RetrieveThing retrieveThing = RetrieveThing.of(thingId,
                DittoHeaders.newBuilder()
                        .correlationId(correlationId)
                        .schemaVersion(JsonSchemaVersion.V_2)
                        .build());

        final C responseConsumer = initResponseConsumer(sourceConnectionName, correlationId);
        final String divertAddress = getTargetAddress(targetConnection);
        final C divertedResponseConsumer = initTargetsConsumer(targetConnectionName, divertAddress);
        sendSignal(sourceConnectionName, retrieveThing);

        // Then: Response should be received at source address
        final M response = consumeResponse(correlationId, responseConsumer);
        assertThat(response).isNotNull();
        // Then: Response should be received at diverted address as well
        final M divertedResponse = consumeFromTarget(targetConnectionName, divertedResponseConsumer);
        assertThat(divertedResponse).isNotNull();

        final Signal<?> signal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(divertedResponse));
        assertThat(signal).isInstanceOf(RetrieveThingResponse.class);
        assertThat(getCorrelationId(divertedResponse)).isEqualTo(correlationId);

        final Signal<?> originalSignal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(response));
        assertThat(originalSignal).isInstanceOf(RetrieveThingResponse.class);
        assertThat(getCorrelationId(response)).isEqualTo(correlationId);
    }

    @Test
    @UseConnection(category = ConnectionCategory.CONNECTION1, mod = DIVERSION_SOURCE)
    @UseConnection(category = ConnectionCategory.CONNECTION2, mod = DIVERSION_TARGET)
    public void divertResponseBasedOnHeaderInTheMessageItself()
            throws ExecutionException, InterruptedException, TimeoutException {
        final ThingId thingId = sendCreateThingAndEnsureResponseIsSentBack(cf.connectionName1,
                ThingBuilder.FromScratch::build);

        final Connection targetConnection = getConnectionExistingByNameForUser(DIVERSION_TARGET,
                getUsername());
        final Connection sourceConnection = getConnectionExistingByNameForUser(DIVERSION_SOURCE,
                getUsername());
        final String targetConnectionId = targetConnection.getId().toString();
        prepareConnectionsForDiversion(sourceConnection, false, false, targetConnection, true);

        // Given
        final String sendingConnectionName = cf.connectionName1;
        final String divertedConnectionName = cf.connectionName2;

        final String correlationId = UUID.randomUUID().toString();
        final String divertAddress = getTargetAddress(targetConnection);
        // When: Send command with divert-response-to-connection header
        final RetrieveThing retrieveThing = RetrieveThing.of(thingId,
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
    public void doNotPublishDivertedResponseIfNotAuthorized()
            throws ExecutionException, InterruptedException, TimeoutException {
        final Connection targetConnection = getConnectionExistingByNameForUser(DIVERSION_TARGET,
                getUsername());
        final Connection sourceConnection = getConnectionExistingByNameForUser(DIVERSION_SOURCE,
                getUsername());

        // Given
        final String sendingConnectionName = cf.connectionName1;
        final String divertedConnectionName = cf.connectionName2;
        final ThingId thingId = sendCreateThingAndEnsureResponseIsSentBack(cf.connectionName1,
                ThingBuilder.FromScratch::build);

        prepareConnectionsForDiversion(sourceConnection, false, false, targetConnection, true);

        final String correlationId = UUID.randomUUID().toString();
        final String divertAddress = getTargetAddress(targetConnection);
        final RetrieveThing retrieveThing = RetrieveThing.of(thingId,
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
    public void testErrorResponseDiversion() throws ExecutionException, InterruptedException, TimeoutException {

        final Connection targetConnection = getConnectionExistingByNameForUser(DIVERSION_TARGET, getUsername());
        final Connection sourceConnection = getConnectionExistingByNameForUser(DIVERSION_SOURCE, getUsername());
        final String sourceConnectionName = cf.connectionWith2Sources;
        final ThingId thingId = sendCreateThingAndEnsureResponseIsSentBack(cf.connectionName2,
                ThingBuilder.FromScratch::build);
        prepareConnectionsForDiversion(sourceConnection, true, false, targetConnection, true);

        // Send command that will fail
        final RetrieveThing retrieveThing = RetrieveThing.of(thingId,
                DittoHeaders.newBuilder()
                        .correlationId(createNewCorrelationId())
                        .schemaVersion(JsonSchemaVersion.V_2)
                        .build());

        final String divertAddress = getTargetAddress(targetConnection);
        final C targetsConsumer = initTargetsConsumer(cf.connectionName1, divertAddress);
        sendSignal(sourceConnectionName + SOURCE1_SUFFIX, retrieveThing);

        // Then: Error response should be received at diverted address
        final M divertedMessage = consumeFromTarget(cf.connectionName1, targetsConsumer);
        assertThat(divertedMessage).isNotNull();

        final Signal<?> errorResponseSignal = PROTOCOL_ADAPTER.fromAdaptable(jsonifiableAdaptableFrom(divertedMessage));
        assertThat(errorResponseSignal).isInstanceOf(ThingErrorResponse.class);
    }

    private String getUsername() {
        return testingContextWithRandomNs.getSolution().getUsername();
    }

    private void prepareConnectionsForDiversion(final Connection sourceConnection,
            final boolean addDiversionHeaderMapping, final boolean preserveResponseInSource,
            final Connection targetConnection, final boolean authorizeSources)
            throws ExecutionException, InterruptedException, TimeoutException {
        ConnectionBuilder sourceConnectionBuilder = sourceConnection.toBuilder();
        if (addDiversionHeaderMapping) {
            sourceConnectionBuilder =
                    addDiversionHeaderMappingToConnectionSource(sourceConnection, targetConnection.getId().toString());
        }
        if (preserveResponseInSource) {
            final HashMap<String, String> sourceSpecificConfig = new HashMap<>(sourceConnection.getSpecificConfig());
            sourceSpecificConfig.put("preserve-normal-response-via-source", "true");
            sourceConnectionBuilder.specificConfig(sourceSpecificConfig);
        }
        final Response response = cf.asyncModifyConnection(sourceConnectionBuilder.build().toJson())
                .get(30, TimeUnit.SECONDS);
        LOGGER.info("Modified {} <{}>. Response: {}", sourceConnection.getName().get(), sourceConnection.getId(),
                response.statusLine());
        assertThat(response.getStatusCode()).satisfies(StatusCodeSuccessfulMatcher.getConsumer());

        if (authorizeSources) {
            final Map<String, String> targetSpecificConfig = new HashMap<>(targetConnection.getSpecificConfig());
            targetSpecificConfig.put("authorized-connections-as-sources", sourceConnection.getId().toString());
            final Response targetConResponse = cf.asyncModifyConnection(
                            targetConnection.toBuilder().specificConfig(targetSpecificConfig).build().toJson())
                    .get(30, TimeUnit.SECONDS);
            LOGGER.info("Modified {} <{}>. Response: {}", targetConnection.getName().get(), targetConnection.getId(),
                    targetConResponse.statusLine());
            assertThat(targetConResponse.getStatusCode()).satisfies(StatusCodeSuccessfulMatcher.getConsumer());
        }

    }

    private static Target removeTopicsFrom(final Target target) {
        return ConnectivityModelFactory.newTargetBuilder(target)
                .topics(Set.of())
                .build();
    }

    private ConnectionBuilder addDiversionHeaderMappingToConnectionSource(final Connection sourceConnection,
            final String targetConnectionId) {
        final List<Source> newSources = sourceConnection.getSources().stream()
                .map(source -> isFirstSource(source, sourceConnection)
                        ? addDiversionHeader(source, targetConnectionId)
                        : source)
                .toList();

        return sourceConnection.toBuilder().setSources(newSources);
    }

    private boolean isFirstSource(final Source source, final Connection connection) {
        return source.equals(connection.getSources().getFirst());
    }

    private Source addDiversionHeader(final Source source, final String targetConnectionId) {
        final Map<String, String> mapping = new HashMap<>(source.getHeaderMapping().getMapping());
        mapping.put(DittoHeaderDefinition.DIVERT_RESPONSE_TO_CONNECTION.getKey(), targetConnectionId);

        return ConnectivityModelFactory.newSourceBuilder(source)
                .headerMapping(ConnectivityModelFactory.newHeaderMapping(mapping))
                .build();
    }

    // Helper methods
    protected String getTargetAddress(final Connection targetConnection) {
        return targetConnection.getTargets().getFirst().getAddress();
    }
}