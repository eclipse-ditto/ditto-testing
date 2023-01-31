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

import static java.util.Objects.requireNonNull;
import static org.eclipse.ditto.testing.system.connectivity.ConnectivityConstants.HEADER_ID;
import static org.eclipse.ditto.things.api.Permission.READ;
import static org.eclipse.ditto.things.api.Permission.WRITE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.eclipse.ditto.base.model.acks.AcknowledgementLabel;
import org.eclipse.ditto.base.model.auth.AuthorizationContext;
import org.eclipse.ditto.base.model.auth.AuthorizationModelFactory;
import org.eclipse.ditto.base.model.auth.AuthorizationSubject;
import org.eclipse.ditto.base.model.auth.DittoAuthorizationContextType;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionBuilder;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.ConnectivityStatus;
import org.eclipse.ditto.connectivity.model.Enforcement;
import org.eclipse.ditto.connectivity.model.FilteredTopic;
import org.eclipse.ditto.connectivity.model.FilteredTopicBuilder;
import org.eclipse.ditto.connectivity.model.HeaderMapping;
import org.eclipse.ditto.connectivity.model.MappingContext;
import org.eclipse.ditto.connectivity.model.PayloadMappingDefinition;
import org.eclipse.ditto.connectivity.model.ReplyTarget;
import org.eclipse.ditto.connectivity.model.Source;
import org.eclipse.ditto.connectivity.model.SourceBuilder;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.connectivity.model.TargetBuilder;
import org.eclipse.ditto.connectivity.model.Topic;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.policies.model.SubjectType;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;

public final class ConnectionModelFactory {

    public static final String DITTO_STATUS_SOURCE_SUFFIX = "_ditto-status";
    public static final String IMPLICIT_THING_CREATION_SOURCE_SUFFIX = "_implicitThingCreation";
    public static final String STATUS_SOURCE_SUFFIX = "_status";
    public static final String JS_SOURCE_SUFFIX = "_js";
    public static final String MERGE_SOURCE_SUFFIX = "_merge";

    public static final String SOURCE1_SUFFIX = "_1";
    public static final String SOURCE2_SUFFIX = "_2";
    public static final String SOURCE3_SUFFIX = "_3";
    private final ConnectionAuthIdentifierFactory authIdentifier;

    /**
     * Convert an address into a valid acknowledgement label matching "[a-zA-Z0-9-_:]{3,100}".
     *
     * @param address a source or target address.
     * @return a valid acknowledgement label.
     */
    public static AcknowledgementLabel toAcknowledgementLabel(final String address) {
        final String escaped = encodeAndTruncateAckLabel(address, 63);
        final String ackLabel = "{{connection:id}}:" + (escaped.length() >= 3 ? escaped : escaped + "---");
        return AcknowledgementLabel.of(ackLabel);
    }

    /**
     * Convert an address into a valid acknowledgement label matching "[a-zA-Z0-9-_:]{3,100}".
     *
     * @param connectionId the concrete connectionId to use as AcknowledgmentLabel prefix
     * @param address a source or target address.
     * @return a valid acknowledgement label.
     */
    public static AcknowledgementLabel toAcknowledgementLabel(final ConnectionId connectionId, final String address) {
        final String escaped = encodeAndTruncateAckLabel(address, 63);
        final String ackLabel = connectionId + ":" + (escaped.length() >= 3 ? escaped : escaped + "---");
        return AcknowledgementLabel.of(ackLabel);
    }

    public ConnectionModelFactory(final ConnectionAuthIdentifierFactory authIdentifier) {
        this.authIdentifier = authIdentifier;
    }

    @FunctionalInterface
    public interface ConnectionAuthIdentifierFactory {

        String createIdentifier(String username);
    }

    /**
     * Builds a connection model for establishing a simple connection (without payload mapping).
     */
    public Connection buildConnectionModel(final String username,
            final String connectionName,
            final ConnectionType connectionType,
            final String connectionUri,
            final Map<String, String> specificConfig,
            @Nullable final String sourceAddress,
            final String targetAddress) {

        return buildConnectionModel(username, connectionName, connectionType, connectionUri, specificConfig,
                sourceAddress, targetAddress, Set.of());
    }

    /**
     * Builds a connection model for establishing a simple connection (without payload mapping).
     */
    public Connection buildConnectionModel(final String username,
            final String connectionName,
            final ConnectionType connectionType,
            final String connectionUri,
            final Map<String, String> specificConfig,
            @Nullable final String sourceAddress,
            final String targetAddress,
            final Collection<String> additionalAuthSubjects) {

        final String authSubject = authIdentifier.createIdentifier(username);
        final List<String> allAuthSubjects = new ArrayList<>();
        allAuthSubjects.add(authSubject);
        allAuthSubjects.addAll(additionalAuthSubjects);
        final List<Source> sources = Optional.ofNullable(sourceAddress)
                .map(address -> buildSource(address, allAuthSubjects.toArray(new String[0])))
                .map(Collections::singletonList)
                .orElse(Collections.emptyList());

        return buildOpenedConnection(connectionName, connectionType, connectionUri)
                .sources(sources)
                .targets(Collections.singletonList(buildTarget(targetAddress, allAuthSubjects.toArray(new String[0]))))
                .specificConfig(specificConfig)
                .build();
    }

    public ConnectionBuilder buildOpenedConnection(final String name, final ConnectionType type,
            final String uri) {
        requireNonNull(name);
        requireNonNull(type);
        requireNonNull(uri);

        final Map<String, String> specificConfig = new HashMap<>();
        specificConfig.put("failover.initialReconnectDelay", String.valueOf(2000));
        specificConfig.put("failover.startupMaxReconnectAttempts", String.valueOf(2));

        final ConnectionId connectionId = ConnectionId.of(name);
        return ConnectivityModelFactory.newConnectionBuilder(connectionId, type, ConnectivityStatus.OPEN, uri)
                .name(name)
                .processorPoolSize(1)
                .failoverEnabled(true)
                .validateCertificate(false)
                .specificConfig(specificConfig);
    }

    /**
     * Builds a connection model for establishing a connection with JavaScript payload mapping.
     */
    Connection buildConnectionModelWithJsMapping(
            final String username,
            final String connectionName,
            final ConnectionType connectionType,
            final String connectionUri,
            final Map<String, String> specificConfig,
            @Nullable final String sourceAddress,
            final String targetAddress) {

        final Connection connectionTemplate = buildConnectionModel(username, connectionName, connectionType,
                connectionUri, specificConfig, sourceAddress, targetAddress);

        final Map<String, String> mappingOptions = new HashMap<>();
        mappingOptions.put("incomingScript", Resources.INCOMING_SCRIPT);
        mappingOptions.put("outgoingScript", Resources.OUTGOING_SCRIPT);
        final MappingContext mappingContext =
                ConnectivityModelFactory.newMappingContext("JavaScript", mappingOptions);

        return connectionTemplate.toBuilder()
                .specificConfig(specificConfig)
                .mappingContext(mappingContext)
                .build();
    }

    Connection buildConnectionModelWith2Sources(
            final String username,
            final String connectionName,
            final ConnectionType connectionType,
            final String connectionUri,
            final Map<String, String> specificConfig,
            final String sourceAddress) {

        final String authSubject = authIdentifier.createIdentifier(username);
        final List<String> allAuthSubjects = new ArrayList<>();
        allAuthSubjects.add(authSubject);
        final List<Source> sources = List.of(buildSource(appendSuffix(connectionName, sourceAddress, SOURCE1_SUFFIX),
                        allAuthSubjects.toArray(new String[0])),
                buildSource(appendSuffix(connectionName, sourceAddress, SOURCE2_SUFFIX),
                        allAuthSubjects.toArray(new String[0])));

        return buildOpenedConnection(connectionName, connectionType, connectionUri)
                .sources(sources)
                .specificConfig(specificConfig)
                .build();
    }

    Connection buildHonoConnectionModel(
            final String username,
            final String connectionName,
            final Map<String, String> specificConfig) {

        final String authSubject = authIdentifier.createIdentifier(username);
        final List<String> allAuthSubjects = new ArrayList<>();
        allAuthSubjects.add(authSubject);
        final List<Source> sources = List.of(
                buildSource("event", allAuthSubjects.toArray(new String[0])),
                buildSource("telemetry", allAuthSubjects.toArray(new String[0])),
                buildSource("command_response", allAuthSubjects.toArray(new String[0])));

        List<Target> targets = List.of(buildTarget("command", allAuthSubjects.toArray(new String[0])));

        return buildOpenedConnection(connectionName, ConnectionType.HONO, "")
                .sources(sources)
                .targets(targets)
                .specificConfig(specificConfig)
                .build();
    }

    Connection buildConnectionModelWithMultipleMappings(final String username, final String connectionId,
            final ConnectionType connectionType, final String connectionUri,
            final Map<String, String> specificConfig, final String sourceAddress,
            final String targetAddress, final AuthClient client) {

        final String integrationSubject = authIdentifier.createIdentifier(username);
        final AuthorizationContext authorizationContext =
                AuthorizationModelFactory.newAuthContext(DittoAuthorizationContextType.UNSPECIFIED,
                        AuthorizationModelFactory.newAuthSubject(integrationSubject));

        final Map<String, String> jsMappingOptions = new HashMap<>();
        jsMappingOptions.put("incomingScript", Resources.INCOMING_SCRIPT);
        jsMappingOptions.put("outgoingScript", Resources.OUTGOING_SCRIPT);
        final MappingContext jsMappingContext =
                ConnectivityModelFactory.newMappingContext("JavaScript", jsMappingOptions);

        final Map<String, String> mergeMappingOptions = new HashMap<>();
        mergeMappingOptions.put("incomingScript", Resources.INCOMING_SCRIPT_MERGE);
        mergeMappingOptions.put("outgoingScript", Resources.OUTGOING_SCRIPT_MERGE);
        final MappingContext mergeMappingContext =
                ConnectivityModelFactory.newMappingContext("JavaScript", mergeMappingOptions);

        final Map<String, String> statusMappingOptions = Collections.singletonMap("thingId", "{{ header:device_id }}");
        final MappingContext statusMappingContext =
                ConnectivityModelFactory.newMappingContext("ConnectionStatus", statusMappingOptions);

        final JsonObject thingTemplateJson = JsonObject.newBuilder()
                .set("thingId", "{{ header:device_id }}")
                .set("policyId", "{{ header:device_id }}")
                .set("_policy", Policy.newBuilder()
                        .forLabel("DEFAULT")
                        .setSubject(client.getDefaultSubject())
                        .setSubject(SubjectIssuer.INTEGRATION, username + ":" + connectionId)
                        .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                        .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                        .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                        .build()
                        .toJson())
                .set("attributes", JsonObject.newBuilder()
                        .set("foo", 42)
                        .build())
                .build();

        final JsonObject standaloneThingTemplate = JsonObject.newBuilder()
                .set("thingId", "{{ header:device_id }}")
                .set("policyId", "{{ header:device_id }}")
                .set("_policy", Policy.newBuilder()
                        .forLabel("DEFAULT")
                        .setSubject(client.getDefaultSubject())
                        .setSubject(SubjectIssuer.INTEGRATION, username + ":" + connectionId)
                        .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                        .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                        .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                        .forLabel("DEVICE-MANAGEMENT")
                        .setSubject("integration:" + username + ":iot-manager",
                                SubjectType.newInstance("ditto-integration"))
                        .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                        .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                        .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                        .build()
                        .toJson())
                .set("attributes", JsonObject.newBuilder()
                        .set("foo", 42)
                        .build())
                .build();

        final String implicitEdgeThingCreation = "implicitEdgeThingCreation";
        final String implicitStandaloneThingCreation = "implicitStandaloneThingCreation";
        final JsonObject implicitThingCreationMappingOptions = JsonObject.newBuilder()
                .set("thing", thingTemplateJson)
                .build();
        final JsonObject implicitStandaloneThingCreationMappingOptions = JsonObject.newBuilder()
                .set("thing", standaloneThingTemplate)
                .build();
        final Map<String, String> implicitThingCreationMappingConditions = Map.of(
                "honoRegistrationStatus", "fn:filter(header:hono_registration_status,'eq','NEW')",
                "behindGateway", "fn:filter(header:gateway_id, 'exists')"
        );
        final Map<String, String> implicitStandaloneThingCreationMappingConditions = Map.of(
                "honoRegistrationStatus", "fn:filter(header:hono_registration_status,'eq','NEW')",
                "notBehindGateway", "fn:filter(header:gateway_id, 'exists', 'false')"
        );
        final MappingContext implicitEdgeThingCreationMappingContext =
                ConnectivityModelFactory.newMappingContext("ImplicitThingCreation",
                        implicitThingCreationMappingOptions,
                        implicitThingCreationMappingConditions,
                        Map.of());
        final MappingContext implicitStandaloneThingCreationMappingContext =
                ConnectivityModelFactory.newMappingContext("ImplicitThingCreation",
                        implicitStandaloneThingCreationMappingOptions,
                        implicitStandaloneThingCreationMappingConditions,
                        Map.of());


        final Map<String, MappingContext> definitions = new HashMap<>();
        definitions.put("js", jsMappingContext);
        definitions.put("merge", mergeMappingContext);
        definitions.put("stat", statusMappingContext);
        definitions.put(implicitEdgeThingCreation, implicitEdgeThingCreationMappingContext);
        definitions.put(implicitStandaloneThingCreation, implicitStandaloneThingCreationMappingContext);

        final PayloadMappingDefinition payloadMappingDefinition =
                ConnectivityModelFactory.newPayloadMappingDefinition(definitions);

        final List<Source> sources;
        if (sourceAddress != null) {
            // build source with ditto + status mapping
            final Source dittoStatusSource = sourceBuilder(appendSuffix(connectionId, sourceAddress,
                    DITTO_STATUS_SOURCE_SUFFIX))
                    .payloadMapping(ConnectivityModelFactory.newPayloadMapping("Ditto", "stat"))
                    .authorizationContext(authorizationContext)
                    .build();

            // build source with implicitThingCreationMapping mapping
            final Source implicitThingCreationSource = sourceBuilder(appendSuffix(connectionId, sourceAddress,
                    IMPLICIT_THING_CREATION_SOURCE_SUFFIX))
                    .payloadMapping(ConnectivityModelFactory.newPayloadMapping(
                            implicitEdgeThingCreation, implicitStandaloneThingCreation))
                    .authorizationContext(authorizationContext)
                    .build();

            // build source with status mapping only
            final Source statusOnlySource =
                    sourceBuilder(appendSuffix(connectionId, sourceAddress, STATUS_SOURCE_SUFFIX))
                            .payloadMapping(ConnectivityModelFactory.newPayloadMapping("stat"))
                            .authorizationContext(authorizationContext)
                            .build();

            // build source with js mapping with multiple messages as result
            final Source jsArrayResultSource =
                    sourceBuilder(appendSuffix(connectionId, sourceAddress, JS_SOURCE_SUFFIX))
                            .payloadMapping(ConnectivityModelFactory.newPayloadMapping("js"))
                            .authorizationContext(authorizationContext)
                            .build();

            // build source with js mapping with that produces a merge update
            final Source jsMergeSource =
                    sourceBuilder(appendSuffix(connectionId, sourceAddress, MERGE_SOURCE_SUFFIX))
                            .payloadMapping(ConnectivityModelFactory.newPayloadMapping("merge"))
                            .authorizationContext(authorizationContext)
                            .build();

            sources = Arrays.asList(dittoStatusSource, implicitThingCreationSource, statusOnlySource,
                    jsArrayResultSource, jsMergeSource);
        } else {
            sources = Collections.emptyList();
        }

        final HeaderMapping targetHeaderMapping;
        if (connectionTypeSupportsHeaders(connectionType)) {
            targetHeaderMapping = ConnectivityModelFactory.newHeaderMapping(JsonObject.newBuilder()
                    .set("index", "{{header:index}}")
                    .set("correlation-id", "{{header:correlation-id}}")
                    .set("content-type", "{{header:content-type}}")
                    .set("reply-to", "{{header:reply-to}}")
                    .build());
        } else {
            targetHeaderMapping = null;
        }
        final Target jsArrayResultTarget = targetBuilder(appendSuffix(connectionId, targetAddress, "_js"))
                .payloadMapping(ConnectivityModelFactory.newPayloadMapping("js"))
                .authorizationContext(authorizationContext)
                .headerMapping(targetHeaderMapping)
                .qos(1)
                .build();

        return buildOpenedConnection(connectionId, connectionType, connectionUri)
                .sources(sources)
                .targets(Collections.singletonList(jsArrayResultTarget))
                .specificConfig(specificConfig)
                .payloadMappingDefinition(payloadMappingDefinition)
                .build();
    }

    private static String appendSuffix(final String connectionId, final String sourceAddress,
            final String sourceSuffix) {
        return sourceAddress.replace(connectionId, connectionId + sourceSuffix);
    }

    /**
     * Builds a connection model for establishing a simple connection (without payload mapping).
     */
    Connection buildConnectionModelWithAuthPlaceholder(
            final String username,
            final String connectionName,
            final ConnectionType connectionType,
            final String connectionUri,
            final Map<String, String> specificConfig,
            @Nullable final String sourceAddress,
            final String targetAddress) {

        final String authSubjectWithPlaceholder = String.format("integration:%s:{{header:%s}}", username, HEADER_ID);
        final String authSubject = authIdentifier.createIdentifier(username);

        final HeaderMapping headerMapping;
        if (connectionTypeSupportsHeaders(connectionType)) {
            headerMapping = ConnectivityModelFactory.newHeaderMapping(JsonObject.newBuilder()
                    .set("source", "{{ request:subjectId }}")
                    .set("correlation-id", "{{ header:correlation-id }}")
                    .set("content-type", "{{ header:content-type }}")
                    .set("reply-to", "{{ header:reply-to }}")
                    .set("feature-id", "{{ feature:id }}")
                    .build());
        } else {
            headerMapping = null;
        }


        final List<Source> sources = Optional.ofNullable(sourceAddress)
                .map(address -> sourceBuilder(address, authSubjectWithPlaceholder, authSubject)
                        .headerMapping(headerMapping)
                        .build())
                .map(Collections::singletonList)
                .orElse(Collections.emptyList());

        return buildOpenedConnection(connectionName, connectionType, connectionUri)
                .sources(sources)
                .targets(Collections.singletonList(
                        ConnectivityModelFactory.newTargetBuilder(buildTarget(targetAddress, authSubject))
                                .headerMapping(headerMapping)
                                .build()))
                .specificConfig(specificConfig)
                .build();
    }

    /**
     * Builds a connection model for establishing a connection and specifying which {@code targetTopics}
     * to consume.
     */
    Connection buildConnectionModelWithTargetTopics(
            final String username,
            final String connectionName,
            final ConnectionType connectionType,
            final String connectionUri,
            final Map<String, String> specificConfig,
            @Nullable final String sourceAddress,
            final String targetAddress,
            final List<String> targetTopics) {
        final Set<FilteredTopic> filteredTopics = targetTopics.stream()
                .map(ConnectivityModelFactory::newFilteredTopic)
                .collect(Collectors.toSet());

        final String authSubject = authIdentifier.createIdentifier(username);
        final List<Source> sources = Optional.ofNullable(sourceAddress)
                .map(address -> buildSource(address, authSubject))
                .map(Collections::singletonList)
                .orElse(Collections.emptyList());

        return buildOpenedConnection(connectionName, connectionType, connectionUri)
                .sources(sources)
                .targets(Collections.singletonList(buildTarget(targetAddress, filteredTopics, authSubject)))
                .specificConfig(specificConfig)
                .build();
    }

    public Connection buildConnectionModelWithEnforcement(
            final String username,
            final String connectionName,
            final ConnectionType connectionType,
            final String connectionUri,
            final Map<String, String> specificConfig,
            @Nullable final String sourceAddress,
            final String targetAddress,
            final Enforcement enforcement) {

        final String authSubject = authIdentifier.createIdentifier(username);

        final List<Source> sources = Optional.ofNullable(sourceAddress)
                .map(address -> sourceBuilder(address, authSubject)
                        .enforcement(enforcement)
                        .build())
                .map(Collections::singletonList)
                .orElse(Collections.emptyList());

        return buildOpenedConnection(connectionName, connectionType, connectionUri)
                .sources(sources)
                .targets(Collections.singletonList(buildTarget(targetAddress, authSubject)))
                .specificConfig(specificConfig)
                .build();
    }

    public Connection buildConnectionModelWithHeaderMapping(
            final String username,
            final String connectionName,
            final ConnectionType connectionType,
            final String connectionUri,
            final Map<String, String> specificConfig,
            @Nullable final String sourceAddress,
            final String targetAddress) {

        final String authSubject = authIdentifier.createIdentifier(username);
        final HeaderMapping headerMapping = getHeaderMappingForConnectionWithHeaderMapping();
        final List<Source> sources = Optional.ofNullable(sourceAddress)
                .map(address -> ConnectivityModelFactory.newSourceBuilder()
                        .address(address)
                        .qos(1)
                        .authorizationContext(
                                AuthorizationModelFactory.newAuthContext(DittoAuthorizationContextType.UNSPECIFIED,
                                        AuthorizationModelFactory.newAuthSubject(authSubject)))
                        .headerMapping(headerMapping)
                        .replyTarget(ReplyTarget.newBuilder()
                                .address("{{ header:correlation-id }}")
                                .headerMapping(getHeaderMappingForReplyTarget())
                                .build())
                        .build())
                .map(Collections::singletonList)
                .orElse(Collections.emptyList());

        final Target target = ConnectivityModelFactory.newTargetBuilder()
                .address(targetAddress)
                .qos(1)
                .topics(Topic.LIVE_MESSAGES, Topic.TWIN_EVENTS)
                .authorizationContext(
                        AuthorizationModelFactory.newAuthContext(DittoAuthorizationContextType.UNSPECIFIED,
                                AuthorizationModelFactory.newAuthSubject(authSubject)))
                .headerMapping(headerMapping)
                .build();

        return buildOpenedConnection(connectionName, connectionType, connectionUri)
                .sources(sources)
                .targets(Collections.singletonList(target))
                .specificConfig(specificConfig)
                .build();
    }

    HeaderMapping getHeaderMappingForConnectionWithHeaderMapping() {
        final Map<String, String> headerMappingMap = new HashMap<>();
        headerMappingMap.put("my-correlation-id", "{{header:OVERWRITE-CORrelation-id}}");
        headerMappingMap.put("uppercase-thing-name", "{{ thing:name | fn:upper() }}");
        headerMappingMap.put("subject", "{{topic:subject}}");
        headerMappingMap.put("original-content-type", "{{header:conteNT-TYPe}}");
        headerMappingMap.put("content-type", "application/vnd.eclipse.ditto+json");
        headerMappingMap.put("correlation-id", "{{header:corrELATION-ID}}");
        headerMappingMap.put("reply-to", "{{header:repLY-TO}}");
        return ConnectivityModelFactory.newHeaderMapping(headerMappingMap);
    }

    public static HeaderMapping getHeaderMappingForReplyTarget() {
        final Map<String, String> headerMappingMap = new HashMap<>();
        headerMappingMap.put("my-response-correlation-id", "{{header:my-correlation-id}}");
        headerMappingMap.put("correlation-id", "{{header:correlation-id}}");
        headerMappingMap.put("content-type", "{{header:content-type}}");
        headerMappingMap.put("reply-to", "{{header:reply-to}}");
        return ConnectivityModelFactory.newHeaderMapping(headerMappingMap);
    }

    /**
     * Builds a connection model for establishing a simple connection.
     */
    public Connection buildConnectionModel(final String username,
            final String connectionName,
            final ConnectionType connectionType,
            final String connectionUri,
            final Map<String, String> specificConfig,
            @Nullable final String sourceAddress,
            final String targetAddress,
            final HeaderMapping headerMapping,
            final MappingContext payloadMapping,
            final Topic topic,
            Topic... additionalTopics) {

        final String authSubject = authIdentifier.createIdentifier(username);
        final List<Source> sources = Optional.ofNullable(sourceAddress)
                .map(address -> ConnectivityModelFactory.newSourceBuilder()
                        .address(address)
                        .qos(1)
                        .authorizationContext(
                                AuthorizationModelFactory.newAuthContext(DittoAuthorizationContextType.UNSPECIFIED,
                                        AuthorizationModelFactory.newAuthSubject(authSubject)))
                        .headerMapping(headerMapping)
                        .replyTarget(ReplyTarget.newBuilder()
                                .address("{{ header:correlation-id }}")
                                .headerMapping(getHeaderMappingForReplyTarget())
                                .build())
                        .build())
                .map(Collections::singletonList)
                .orElse(Collections.emptyList());

        final Target target = ConnectivityModelFactory.newTargetBuilder()
                .address(targetAddress)
                .qos(1)
                .topics(topic, additionalTopics)
                .authorizationContext(
                        AuthorizationModelFactory.newAuthContext(DittoAuthorizationContextType.UNSPECIFIED,
                                AuthorizationModelFactory.newAuthSubject(authSubject)))
                .headerMapping(headerMapping)
                .build();

        return buildOpenedConnection(connectionName, connectionType, connectionUri)
                .sources(sources)
                .targets(Collections.singletonList(target))
                .specificConfig(specificConfig)
                .mappingContext(payloadMapping)
                .build();
    }

    private static Source buildSource(final String sourceAddress, final String... authorisationSubjects) {
        requireNonNull(sourceAddress);
        requireNonNull(authorisationSubjects);

        return sourceBuilder(sourceAddress, authorisationSubjects).build();
    }

    private static SourceBuilder sourceBuilder(final String sourceAddress, final String... authorisationSubjects) {
        requireNonNull(sourceAddress);
        requireNonNull(authorisationSubjects);

        final AuthorizationContext authContext = getAuthorizationSubjects(authorisationSubjects);

        return ConnectivityModelFactory.newSourceBuilder()
                .address(sourceAddress)
                .authorizationContext(authContext)
                .declaredAcknowledgementLabels(Collections.singleton(
                        AcknowledgementLabel.of("{{connection:id}}:custom")))
                .qos(1)
                .consumerCount(1);
    }

    @Nonnull
    private static AuthorizationContext getAuthorizationSubjects(final String[] authorisationSubjects) {
        final List<AuthorizationSubject> authorizationSubjectList = Arrays.stream(authorisationSubjects)
                .map(AuthorizationSubject::newInstance)
                .collect(Collectors.toList());
        return AuthorizationModelFactory.newAuthContext(DittoAuthorizationContextType.UNSPECIFIED,
                authorizationSubjectList);
    }

    private static TargetBuilder targetBuilder(final String targetAddress, final String... authorisationSubjects) {
        return ConnectivityModelFactory.newTargetBuilder()
                .address(targetAddress)
                .authorizationContext(getAuthorizationSubjects(authorisationSubjects))
                .topics(Topic.TWIN_EVENTS);
    }

    private static Target buildTarget(final String targetAddress, final String... authorisationSubjects) {
        requireNonNull(targetAddress);
        requireNonNull(authorisationSubjects);

        final Set<FilteredTopic> topics =
                Stream.of(Topic.TWIN_EVENTS, Topic.LIVE_MESSAGES, Topic.LIVE_COMMANDS, Topic.POLICY_ANNOUNCEMENTS)
                        .map(ConnectivityModelFactory::newFilteredTopicBuilder)
                        .map(builder -> builder.withFilter("not(like(thingId,'*filter-events'))"))
                        .map(FilteredTopicBuilder::build)
                        .collect(Collectors.toSet());

        return buildTarget(targetAddress, topics, authorisationSubjects);
    }

    private static Target buildTarget(final String targetAddress, final Set<FilteredTopic> topics,
            final String... authorisationSubjects) {
        requireNonNull(targetAddress);
        requireNonNull(topics);
        requireNonNull(authorisationSubjects);

        final AuthorizationContext authContext = getAuthorizationSubjects(authorisationSubjects);

        return ConnectivityModelFactory.newTargetBuilder()
                .address(targetAddress)
                .authorizationContext(authContext)
                .qos(2) // use QoS 2 because command order is tested for connection1
                .topics(topics)
                .issuedAcknowledgementLabel(toAcknowledgementLabel(targetAddress))
                .build();
    }

    private boolean connectionTypeSupportsHeaders(final ConnectionType type) {
        return !ConnectionType.MQTT.equals(type);
    }

    private static String encodeAndTruncateAckLabel(final String address, final int maxAckLabelLength) {
        return address.chars()
                .limit(maxAckLabelLength)
                .mapToObj(i -> Character.isAlphabetic(i) || Character.isDigit(i)
                        ? new String(Character.toChars(i))
                        : "-")
                .collect(Collectors.joining());
    }

}
