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

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.common.ResponseType;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.ConnectivityStatus;
import org.eclipse.ditto.connectivity.model.Enforcement;
import org.eclipse.ditto.connectivity.model.HeaderMapping;
import org.eclipse.ditto.connectivity.model.MappingContext;
import org.eclipse.ditto.connectivity.model.ReplyTarget;
import org.eclipse.ditto.connectivity.model.Source;
import org.eclipse.ditto.connectivity.model.SshTunnel;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.connectivity.model.Topic;
import org.eclipse.ditto.connectivity.model.signals.commands.query.RetrieveConnectionStatusResponse;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.Solution;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.client.ConnectionsClient;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.response.Response;

public final class ConnectivityFactory {

    private static final AtomicInteger DISAMBIGUATION_COUNTER =
            new AtomicInteger(ThreadLocalRandom.current().nextInt(0, 0x10000));

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectivityFactory.class);
    private static final Duration LIVE_STATUS_POLL_TIMEOUT = Duration.ofSeconds(30);
    private static final AtomicInteger CONNECTION_RETRY_COUNTER = new AtomicInteger();
    private static final int DEFAULT_MAX_CLIENT_COUNT = 3;

    private final ConnectionModelFactory modelBuilder;
    private final ConnectionType connectionType;

    private final GetConnectionUri getConnectionUri;
    private final GetSpecificConfig getSpecificConfig;
    private final GetTargetAddress getTargetAddress;
    private final GetSourceAddress getSourceAddress;
    private final GetEnforcement getEnforcement;
    private final GetSshTunnel getSshTunnel;
    private final AuthClient client;
    private final SolutionSupplier solutionSupplier;
    private final Map<String, String> defaultHeaderMapping;
    private final GetReplyTargetAddress getReplyTargetAddress;
    private final Map<String, ConnectionId> connectionIds = new ConcurrentHashMap<>();
    private final int maxClientCount;

    // All connection names.
    // On adding a new connection:
    // 1. Add its name to both constructors.
    // 2. Add its name to this.allConnectionNames(String...).
    // 3. If the connection has nonstandard source or target addresses, add those to
    //    RabbitMQConnectivityIT#declareExchangesAndQueues()
    //    similar to those of connectionNameWithMultiplePayloadMappings or connectionNameWithAuthPlaceholderOnHEADER_ID.
    public final String connectionName1;
    public final String connectionName2;
    public final String connectionNameWithPayloadMapping;
    public final String connectionNameWithMultiplePayloadMappings;
    public final String connectionNameWithAuthPlaceholderOnHEADER_ID;
    public final String connectionNameWithNamespaceAndRqlFilter;
    public final String connectionNameWithEnforcementEnabled;
    public final String connectionNameWithHeaderMapping;
    public final String connectionNameWithExtraFields;
    public final String connectionWithRawMessageMapper1;
    public final String connectionWithRawMessageMapper2;
    public final String connectionWithTunnel;
    public final String connectionWithConnectionAnnouncements;
    public final String connectionWith2Sources;
    public final String connectionHonoName;

    private ConnectivityFactory(
            final String connectionNamePrefix,
            final ConnectionModelFactory modelBuilder,
            final SolutionSupplier solutionSupplier,
            final ConnectionType connectionType,
            final GetConnectionUri getConnectionUri,
            final GetSpecificConfig getSpecificConfig,
            final GetTargetAddress getTargetAddress,
            final GetSourceAddress getSourceAddress,
            final GetEnforcement getEnforcement,
            final GetSshTunnel getSshTunnel,
            final AuthClient client) {

        this.modelBuilder = modelBuilder;
        this.solutionSupplier = solutionSupplier;
        this.connectionType = connectionType;
        this.getConnectionUri = getConnectionUri;
        this.getSpecificConfig = getSpecificConfig;
        this.getTargetAddress = getTargetAddress;
        this.getSourceAddress = getSourceAddress;
        this.getEnforcement = getEnforcement;
        this.getSshTunnel = getSshTunnel;
        this.client = client;
        this.defaultHeaderMapping = Collections.emptyMap();
        this.getReplyTargetAddress = name -> null;
        maxClientCount = DEFAULT_MAX_CLIENT_COUNT;
        connectionName1 = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 1
        );
        connectionName2 = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 2
        );
        connectionNameWithPayloadMapping = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 3
        );
        connectionNameWithAuthPlaceholderOnHEADER_ID = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 4
        );
        connectionNameWithNamespaceAndRqlFilter = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 5
        );
        connectionNameWithEnforcementEnabled = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 6
        );
        connectionNameWithHeaderMapping = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 7
        );
        connectionNameWithMultiplePayloadMappings = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 8
        );
        connectionNameWithExtraFields = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 9
        );
        connectionWithRawMessageMapper1 = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 10
        );
        connectionWithRawMessageMapper2 = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 11
        );
        connectionWithTunnel = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 12
        );
        connectionWithConnectionAnnouncements = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 13
        );
        connectionWith2Sources = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 14
        );
        connectionHonoName = disambiguateConnectionName(
                solutionSupplier.getSolution().getUsername(), connectionNamePrefix + 15
        );
    }

    // copy constructor
    private ConnectivityFactory(
            final ConnectivityFactory cf,
            final ConnectionModelFactory modelBuilder,
            final SolutionSupplier solutionSupplier,
            final ConnectionType connectionType,
            final GetConnectionUri getConnectionUri,
            final GetSpecificConfig getSpecificConfig,
            final GetTargetAddress getTargetAddress,
            final GetSourceAddress getSourceAddress,
            final Map<String, String> defaultHeaderMapping,
            final GetReplyTargetAddress getReplyTargetAddress,
            final int maxClientCount) {

        this.modelBuilder = modelBuilder;
        this.solutionSupplier = solutionSupplier;
        this.connectionType = connectionType;
        this.getConnectionUri = getConnectionUri;
        this.getSpecificConfig = getSpecificConfig;
        this.getTargetAddress = getTargetAddress;
        this.getSourceAddress = getSourceAddress;
        this.defaultHeaderMapping = defaultHeaderMapping;
        this.getReplyTargetAddress = getReplyTargetAddress;

        getSshTunnel = cf.getSshTunnel;
        getEnforcement = cf.getEnforcement;
        client = cf.client;
        connectionName1 = cf.connectionName1;
        connectionName2 = cf.connectionName2;
        connectionNameWithPayloadMapping = cf.connectionNameWithPayloadMapping;
        connectionNameWithAuthPlaceholderOnHEADER_ID = cf.connectionNameWithAuthPlaceholderOnHEADER_ID;
        connectionNameWithNamespaceAndRqlFilter = cf.connectionNameWithNamespaceAndRqlFilter;
        connectionNameWithEnforcementEnabled = cf.connectionNameWithEnforcementEnabled;
        connectionNameWithHeaderMapping = cf.connectionNameWithHeaderMapping;
        connectionNameWithMultiplePayloadMappings = cf.connectionNameWithMultiplePayloadMappings;
        connectionNameWithExtraFields = cf.connectionNameWithExtraFields;
        connectionWithRawMessageMapper1 = cf.connectionWithRawMessageMapper1;
        connectionWithRawMessageMapper2 = cf.connectionWithRawMessageMapper2;
        connectionWithTunnel = cf.connectionWithTunnel;
        connectionWithConnectionAnnouncements = cf.connectionWithConnectionAnnouncements;
        connectionWith2Sources = cf.connectionWith2Sources;
        connectionHonoName = cf.connectionHonoName;
        this.maxClientCount = maxClientCount;
    }

    public static ConnectivityFactory of(
            final String connectionNamePrefix,
            final ConnectionModelFactory modelBuilder,
            final SolutionSupplier solutionSupplier,
            final ConnectionType connectionType,
            final GetConnectionUri getConnectionUri,
            final GetSpecificConfig getSpecificConfig,
            final GetTargetAddress getTargetAddress,
            final GetSourceAddress getSourceAddress,
            final GetEnforcement getEnforcement,
            final GetSshTunnel getSshTunnel,
            final AuthClient client) {

        return new ConnectivityFactory(connectionNamePrefix, modelBuilder, solutionSupplier, connectionType,
                getConnectionUri, getSpecificConfig, getTargetAddress, getSourceAddress, getEnforcement, getSshTunnel,
                client);
    }

    public ConnectionId getConnectionId(final String connectionName) {
        return connectionIds.get(connectionName);
    }

    public String getConnectionName(final ConnectionCategory category) {
        return switch (category) {
            case CONNECTION1 -> connectionName1;
            case CONNECTION2 -> connectionName2;
            case CONNECTION_WITH_PAYLOAD_MAPPING -> connectionNameWithPayloadMapping;
            case CONNECTION_WITH_AUTH_PLACEHOLDER_ON_HEADER_ID -> connectionNameWithAuthPlaceholderOnHEADER_ID;
            case CONNECTION_WITH_NAMESPACE_AND_RQL_FILTER -> connectionNameWithNamespaceAndRqlFilter;
            case CONNECTION_WITH_ENFORCEMENT_ENABLED -> connectionNameWithEnforcementEnabled;
            case CONNECTION_WITH_HEADER_MAPPING -> connectionNameWithHeaderMapping;
            case CONNECTION_WITH_MULTIPLE_PAYLOAD_MAPPINGS -> connectionNameWithMultiplePayloadMappings;
            case CONNECTION_WITH_EXTRA_FIELDS -> connectionNameWithExtraFields;
            case CONNECTION_WITH_RAW_MESSAGE_MAPPER_1 -> connectionWithRawMessageMapper1;
            case CONNECTION_WITH_RAW_MESSAGE_MAPPER_2 -> connectionWithRawMessageMapper2;
            case CONNECTION_WITH_SSH_TUNNEL -> connectionWithTunnel;
            case CONNECTION_WITH_CONNECTION_ANNOUNCEMENTS -> connectionWithConnectionAnnouncements;
            case CONNECTION_WITH_2_SOURCES -> connectionWith2Sources;
            case CONNECTION_HONO -> connectionHonoName;
            case NONE -> "noname";
        };
    }

    public String[] allConnectionNames(final String... extraNames) {
        return Stream.of(
                        new String[]{
                                connectionName1,
                                connectionName2,
                                connectionNameWithPayloadMapping,
                                connectionNameWithMultiplePayloadMappings,
                                connectionNameWithAuthPlaceholderOnHEADER_ID,
                                connectionNameWithNamespaceAndRqlFilter,
                                connectionNameWithEnforcementEnabled,
                                connectionNameWithHeaderMapping,
                                connectionNameWithExtraFields,
                                connectionWithRawMessageMapper1,
                                connectionWithRawMessageMapper2,
                                connectionWithTunnel,
                                connectionWithConnectionAnnouncements,
                                connectionWith2Sources,
                                connectionHonoName
                        },
                        extraNames)
                .flatMap(Arrays::stream)
                .toArray(String[]::new);

    }

    /**
     * Add a reply target address. Will only be effective if default header mapping is non-empty, because it will
     * disable live migration.
     *
     * @param defaultReplyTargetAddress the reply target address.
     * @return a copy of this factory with default target address.
     */
    public ConnectivityFactory withDefaultReplyTargetAddress(@Nullable final String defaultReplyTargetAddress) {
        if (defaultHeaderMapping.isEmpty()) {
            throw new IllegalArgumentException("Setting defaultReplyTargetAddress has no effect without " +
                    "setting a nonempty defaultHeaderMapping first.");
        }
        return new ConnectivityFactory(this, modelBuilder, solutionSupplier, connectionType, getConnectionUri,
                getSpecificConfig, getTargetAddress, getSourceAddress, defaultHeaderMapping,
                name -> defaultReplyTargetAddress, maxClientCount);
    }

    public ConnectivityFactory withReplyTargetAddress(final GetReplyTargetAddress getReplyTargetAddress) {
        if (defaultHeaderMapping.isEmpty()) {
            throw new IllegalArgumentException("Setting getReplyTargetAddress has no effect without " +
                    "setting a nonempty defaultHeaderMapping first.");
        }
        return new ConnectivityFactory(this, modelBuilder, solutionSupplier, connectionType, getConnectionUri,
                getSpecificConfig, getTargetAddress, getSourceAddress, defaultHeaderMapping, getReplyTargetAddress,
                maxClientCount);
    }


    public ConnectivityFactory withDefaultHeaderMapping(final Map<String, String> defaultHeaderMapping) {
        return new ConnectivityFactory(this, modelBuilder, solutionSupplier, connectionType, getConnectionUri,
                getSpecificConfig, getTargetAddress, getSourceAddress, defaultHeaderMapping, getReplyTargetAddress,
                maxClientCount);
    }

    public ConnectivityFactory withTargetAddress(final GetTargetAddress getTargetAddress) {
        return new ConnectivityFactory(this, modelBuilder, solutionSupplier, connectionType, getConnectionUri,
                getSpecificConfig, getTargetAddress, getSourceAddress, defaultHeaderMapping, getReplyTargetAddress,
                maxClientCount);
    }

    public ConnectivityFactory withMaxClientCount(final int maxClientCount) {
        return new ConnectivityFactory(this, modelBuilder, solutionSupplier, connectionType, getConnectionUri,
                getSpecificConfig, getTargetAddress, getSourceAddress, defaultHeaderMapping, getReplyTargetAddress,
                maxClientCount);
    }

    public void setUpConnections(final Map<ConnectionCategory, String> enabledConnections) throws Exception {
        setUpConnections(enabledConnections, Collections.emptySet());
    }

    public void setUpConnections(final Map<ConnectionCategory, String> enabledConnections,
            final Set<Connection> additionalConnections) throws Exception {

        if (enabledConnections.isEmpty()) {
            throw new IllegalArgumentException("Add @Connections annotation to specify required connections.");
        }

        final Set<ConnectionCategory> filter = enabledConnections.keySet();
        LOGGER.info("Setting up the following connections: {}", filter);
        final Map<ConnectionCategory, Supplier<Connection>> preparedConnections = buildConnectionSupplierMap();

        CompletableFuture.allOf(
                        Stream.concat(
                                        preparedConnections.entrySet().stream()
                                                .filter(e -> filter.contains(e.getKey()))
                                                .peek(e -> LOGGER.debug("Connection {} will be created.", e.getKey()))
                                                .map(e -> entry(e.getKey(), e.getValue().get()))
                                                .map(e -> Optional.ofNullable(enabledConnections.get(e.getKey()))
                                                        .filter(m -> !m.isEmpty())
                                                        .map(AbstractConnectivityITBase.MODS::get)
                                                        .map(mod -> mod.apply(e.getValue()))
                                                        .orElse(e.getValue())), additionalConnections.stream())
                                .map(this::asyncCreateConnection)
                                .toArray(CompletableFuture[]::new))
                .get(200, TimeUnit.SECONDS);
    }

    private Map<ConnectionCategory, Supplier<Connection>> buildConnectionSupplierMap() {
        final String username = solutionSupplier.getSolution().getUsername();
        return Map.ofEntries(
                entry(ConnectionCategory.CONNECTION1, () -> getSingleConnection(username, connectionName1,
                        Set.of("integration:" + username + ":" + TestingContext.DEFAULT_SCOPE))),
                entry(ConnectionCategory.CONNECTION2, () -> setupSingleConnectionReceivingNackResponses(connectionName2, maxClientCount)),
                entry(ConnectionCategory.CONNECTION_WITH_PAYLOAD_MAPPING,
                        () -> setupSingleConnectionWithPayloadMapping(connectionNameWithPayloadMapping)),
                entry(ConnectionCategory.CONNECTION_WITH_MULTIPLE_PAYLOAD_MAPPINGS,
                        () -> setupSingleConnectionWithMultiplePayloadMappings(
                                connectionNameWithMultiplePayloadMappings)),
                entry(ConnectionCategory.CONNECTION_WITH_AUTH_PLACEHOLDER_ON_HEADER_ID,
                        () -> setupSingleConnectionWithAuthPlaceholder(connectionNameWithAuthPlaceholderOnHEADER_ID)),
                entry(ConnectionCategory.CONNECTION_WITH_NAMESPACE_AND_RQL_FILTER,
                        () -> setupSingleConnectionWithNamespaceAndRqlFilter(connectionNameWithNamespaceAndRqlFilter)),
                entry(ConnectionCategory.CONNECTION_WITH_ENFORCEMENT_ENABLED,
                        () -> setupSingleConnectionWithEnforcement(connectionNameWithEnforcementEnabled,
                                getEnforcement.get(connectionNameWithEnforcementEnabled))),
                entry(ConnectionCategory.CONNECTION_WITH_EXTRA_FIELDS,
                        () -> setupSingleConnectionWithExtraFields(connectionNameWithExtraFields)),
                entry(ConnectionCategory.CONNECTION_WITH_HEADER_MAPPING,
                        () -> setupSingleConnectionWithHeaderMapping(connectionNameWithHeaderMapping)),
                entry(ConnectionCategory.CONNECTION_WITH_RAW_MESSAGE_MAPPER_1,
                        () -> setupConnectionWithRawMessageMappingInSourceAndTarget(connectionWithRawMessageMapper1)),
                entry(ConnectionCategory.CONNECTION_WITH_RAW_MESSAGE_MAPPER_2,
                        () -> setupConnectionWithRawMessageMappingInSourceAndTarget(connectionWithRawMessageMapper2)),
                entry(ConnectionCategory.CONNECTION_WITH_SSH_TUNNEL,
                        () -> getSingleConnectionWithTunnel(connectionWithTunnel)),
                entry(ConnectionCategory.CONNECTION_WITH_CONNECTION_ANNOUNCEMENTS,
                        () -> getSingleConnectionWithConnectionAnnouncement(connectionWithConnectionAnnouncements)),
                entry(ConnectionCategory.CONNECTION_WITH_2_SOURCES, () -> getConnectionWith2Sources(connectionWith2Sources)),
                entry(ConnectionCategory.CONNECTION_HONO, () -> getConnectionHono(connectionHonoName))
        );
    }

    public CompletableFuture<Response> setupSingleConnection(final String connectionName) {
        return setupSingleConnection(solutionSupplier.getSolution(), connectionName);
    }

    public CompletableFuture<Response> setupSingleConnection(final Solution solution, final String connectionName) {
        return setupSingleConnection(solution, connectionName, Set.of());
    }

    public Connection setupSingleConnectionReceivingNackResponses(final String connectionName,
            final int clientCount) {
        final Solution solution = solutionSupplier.getSolution();
        LOGGER.info("Creating a connection of type <{}> with Name <{}> to <{}> in Ditto Connectivity " +
                        "with clientCount={}",
                connectionType, connectionName, getConnectionUri(), clientCount);

        final Connection singleConnection = getSingleConnection(solution.getUsername(), connectionName);
        return singleConnection.toBuilder()
                .clientCount(clientCount)
                .setSources(singleConnection.getSources().stream()
                        .map(source -> ConnectivityModelFactory.newSourceBuilder(source)
                                .replyTarget(source.getReplyTarget()
                                        .orElseThrow(() -> new IllegalStateException("Missing reply target."))
                                        .toBuilder()
                                        .expectedResponseTypes(ResponseType.RESPONSE, ResponseType.ERROR,
                                                ResponseType.NACK)
                                        .build())
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }

    public Connection getSingleConnection(final String username, final String connectionName) {
        return getSingleConnection(username, connectionName, Set.of());
    }

    public Connection getSingleConnection(final String username, final String connectionName,
            final Collection<String> additionalAuthSubjects) {
        return modelBuilder.buildConnectionModel(username,
                connectionName,
                connectionType,
                getConnectionUri(),
                getSpecificConfig(),
                defaultSourceAddress(connectionName),
                defaultTargetAddress(connectionName),
                additionalAuthSubjects);
    }

    public Connection getSingleConnectionWithTunnel(final String connectionName) {
        return getSingleConnection(connectionName).toBuilder()
                .uri(getConnectionUri.get(true, true))
                .sshTunnel(getSshTunnel.getSshTunnel())
                .build();
    }

    public Connection getSingleConnectionWithConnectionAnnouncement(final String connectionId) {
        final HeaderMapping headerMapping = modelBuilder.getHeaderMappingForConnectionWithHeaderMapping();

        final Map<String, String> mappingOptions = new HashMap<>();
        mappingOptions.put("outgoingScript", Resources.OUTGOING_SCRIPT_CONNECTION_ANNOUNCEMENTS);
        final MappingContext payloadMapping = ConnectivityModelFactory.newMappingContext("JavaScript", mappingOptions);

        return modelBuilder.buildConnectionModel(
                solutionSupplier.getSolution().getUsername(),
                connectionId,
                connectionType,
                getConnectionUri(),
                getSpecificConfig(),
                null,
                defaultTargetAddress(connectionId),
                headerMapping,
                payloadMapping,
                Topic.CONNECTION_ANNOUNCEMENTS);
    }

    public Connection getConnectionWith2Sources(final String connectionId) {

        return modelBuilder.buildConnectionModelWith2Sources(
                solutionSupplier.getSolution().getUsername(),
                connectionId,
                connectionType,
                getConnectionUri(),
                getSpecificConfig(),
                defaultSourceAddress(connectionId)
        );
    }

    public Connection getConnectionHono(final String connectionId) {
        return getSingleConnection(connectionId);
    }

    public Connection getSingleConnection(final String connectionName) {
        return modelBuilder.buildConnectionModel(solutionSupplier.getSolution().getUsername(),
                connectionName,
                connectionType,
                getConnectionUri(),
                getSpecificConfig(),
                defaultSourceAddress(connectionName),
                defaultTargetAddress(connectionName));
    }

    public CompletableFuture<Response> setupSingleConnection(final Solution solution, final String connectionName,
            final Collection<String> additionalAuthSubjects) {

        LOGGER.info("Creating a connection of type <{}> with Name <{}> to <{}> in Ditto Connectivity",
                connectionType, connectionName, getConnectionUri());

        return asyncCreateConnection(solution,
                getSingleConnection(solution.getUsername(), connectionName, additionalAuthSubjects));
    }

    public Connection setupSingleConnectionWithAuthPlaceholder(final String connectionId) {
        LOGGER.info("Creating a connection of type <{}> with ID <{}> to <{}> in Ditto Connectivity",
                connectionType, connectionId, getConnectionUri());

        return modelBuilder.buildConnectionModelWithAuthPlaceholder(
                solutionSupplier.getSolution().getUsername(),
                connectionId,
                connectionType,
                getConnectionUri(),
                getSpecificConfig(),
                defaultSourceAddress(connectionId),
                defaultTargetAddress(connectionId) + ConnectivityConstants.TARGET_SUFFIX_PLACEHOLDER);
    }

    public Connection setupSingleConnectionWithPayloadMapping(final String connectionId) {
        LOGGER.info("Creating a connection of type <{}> with payload mapping with Name <{}> to <{}> in Ditto " +
                "Connectivity", connectionType, connectionId, getConnectionUri());
        return modelBuilder.buildConnectionModelWithJsMapping(
                solutionSupplier.getSolution().getUsername(),
                connectionId,
                connectionType, getConnectionUri(),
                getSpecificConfig(),
                defaultSourceAddress(connectionId),
                defaultTargetAddress(connectionId));
    }

    public Connection setupSingleConnectionWithMultiplePayloadMappings(final String connectionId) {
        LOGGER.info("Creating a connection of type <{}> with payload mapping with Name <{}> to <{}> in Ditto " +
                "Connectivity", connectionType, connectionId, getConnectionUri());
        return modelBuilder.buildConnectionModelWithMultipleMappings(
                solutionSupplier.getSolution().getUsername(),
                connectionId,
                connectionType, getConnectionUri(),
                getSpecificConfig(),
                defaultSourceAddress(connectionId),
                defaultTargetAddress(connectionId),
                client);
    }

    public Connection setupSingleConnectionWithNamespaceAndRqlFilter(final String connectionId) {

        LOGGER.info("Creating a connection of type <{}> with namespace and RQL filter with ID <{}> to <{}> in Ditto " +
                "Connectivity", connectionType, connectionId, getConnectionUri());

        return modelBuilder.buildConnectionModelWithTargetTopics(
                solutionSupplier.getSolution().getUsername(),
                connectionId,
                connectionType,
                getConnectionUri(),
                getSpecificConfig(),
                defaultSourceAddress(connectionId),
                defaultTargetAddress(connectionId),
                Collections.singletonList(
                        "_/_/things/twin/events?namespaces=" + AbstractConnectivityITBase.RANDOM_NAMESPACE +
                                "&filter=gt(attributes/counter,42)"
                )
        );
    }

    public Connection setupSingleConnectionWithExtraFields(final String connectionId) {

        LOGGER.info("Creating a connection of type <{}> with extra fields with ID <{}> to <{}>" +
                " in Ditto Connectivity", connectionType, connectionId, getConnectionUri());

        final String extraFields =
                "attributes/counter,features/*/properties/counter,features/{{feature:id}}/properties/connected";
        return modelBuilder.buildConnectionModelWithTargetTopics(
                solutionSupplier.getSolution().getUsername(),
                connectionId,
                connectionType,
                getConnectionUri(),
                getSpecificConfig(),
                defaultSourceAddress(connectionId),
                defaultTargetAddress(connectionId),
                Arrays.asList(
                        "_/_/things/twin/events?extraFields=" + extraFields +
                                "&filter=not(like(thingId,'*filter-events'))",
                        "_/_/things/live/commands?extraFields=" + extraFields + "&filter=eq(attributes/counter,20)",
                        "_/_/things/live/events?extraFields=" + extraFields +
                                "&filter=and(eq(attributes/counter,20),eq(attributes/filter,true))"
                )
        );
    }

    public Connection setupSingleConnectionWithEnforcement(final String connectionId,
            final Enforcement enforcement) {

        LOGGER.info("Creating a connection of type <{}> with enforcement with ID <{}> to <{}> in Ditto " +
                "Connectivity", connectionType, connectionId, getConnectionUri());
        return modelBuilder.buildConnectionModelWithEnforcement(
                solutionSupplier.getSolution().getUsername(),
                connectionId,
                connectionType,
                getConnectionUri(),
                getSpecificConfig(),
                defaultSourceAddress(connectionId),
                defaultTargetAddress(connectionId),
                enforcement);
    }

    public Connection setupSingleConnectionWithHeaderMapping(final String connectionId) {
        LOGGER.info("Creating a connection of type <{}> with headerMapping with ID <{}> to <{}> in Ditto " +
                "Connectivity", connectionType, connectionId, getConnectionUri());

        return modelBuilder.buildConnectionModelWithHeaderMapping(
                solutionSupplier.getSolution().getUsername(),
                connectionId,
                connectionType,
                getConnectionUri(),
                getSpecificConfig(),
                defaultSourceAddress(connectionId),
                defaultTargetAddress(connectionId));
    }

    private Connection setupConnectionWithRawMessageMappingInSourceAndTarget(
            final String connectionName) {
        final Connection temp = getSingleConnection(connectionName);
        final List<Source> sources = temp.getSources().stream()
                .map(source -> ConnectivityModelFactory.newSourceBuilder(source)
                        .payloadMapping(ConnectivityModelFactory.newPayloadMapping("raw"))
                        .build())
                .collect(Collectors.toList());
        final List<Target> targets = temp.getTargets().stream()
                .map(target -> ConnectivityModelFactory.newTargetBuilder(target)
                        .topics(Topic.LIVE_MESSAGES, Topic.LIVE_COMMANDS)
                        .payloadMapping(ConnectivityModelFactory.newPayloadMapping("raw"))
                        .build())
                .collect(Collectors.toList());
        return ConnectivityModelFactory.newConnectionBuilder(temp)
                .setSources(sources)
                .setTargets(targets)
                .payloadMappingDefinition(ConnectivityModelFactory.newPayloadMappingDefinition(Map.of(
                        "raw",
                        ConnectivityModelFactory.newMappingContextBuilder("RawMessage", JsonObject.newBuilder()
                                .set(JsonPointer.of("incomingMessageHeaders/content-type"),
                                        "{{header:content-type|fn:default('application/vnd.eclipse.ditto+json')}}")
                                .build()).build()
                )))
                .build();
    }

    public CompletableFuture<Response> asyncCreateConnection(final Connection connection) {
        return asyncCreateConnection(solutionSupplier.getSolution(), connection);
    }

    public CompletableFuture<Response> asyncCreateConnection(final Solution solution, final Connection connection) {
        return asyncCreateConnection(solution, appendDefaultHeaderMappingAndReplyTarget(connection).toJson());
    }

    /*
    Not allowed to POST a connection with an ID.
     */
    public static JsonObject removeIdFromJson(final JsonObject connectionJson) {
        return JsonFactory.newObject(connectionJson.toString()).toBuilder()
                .remove(Connection.JsonFields.ID)
                .build();
    }

    private static String getConnectionName(final JsonObject connectionJson) {
        return connectionJson.getValue(Connection.JsonFields.NAME).orElse("UNKNOWN");
    }

    private static ConnectivityStatus getDesiredConnectionStatus(final JsonObject connectionJson) {
        return connectionJson.getValue(Connection.JsonFields.CONNECTION_STATUS).flatMap(ConnectivityStatus::forName)
                .orElse(ConnectivityStatus.UNKNOWN);
    }

    public CompletableFuture<Response> asyncCreateConnection(final Solution solution, final JsonObject connection) {

        final JsonObject connectionJsonWithoutId = removeIdFromJson(connection);
        final String connectionName = getConnectionName(connection);
        final ConnectivityStatus desiredStatus = getDesiredConnectionStatus(connection);

        return CompletableFuture.supplyAsync(() ->
                        ConnectionsClient.getInstance()
                                .postConnection(connectionJsonWithoutId)
                                .withDevopsAuth()
                                .withHeader(HttpHeader.TIMEOUT, 60)
                                .expectingHttpStatus(HttpStatus.CREATED)
                                .fire())
                .thenApply(response -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("setup connection <{}> resulted in response: {} headers: {}", connectionName,
                                response.statusCode(), response.headers());
                        response.prettyPrint();
                    } else {
                        LOGGER.info("setup connection <{}> resulted in: {}", connectionName, response.statusLine());
                    }
                    JsonObject.of(response.body().asString())
                            .getValue(Connection.JsonFields.ID.getPointer())
                            .ifPresent(id -> connectionIds.put(connectionName, ConnectionId.of(id.asString())));
                    return response;
                })
                .thenCompose(response -> {
                    final String connectionId = JsonFactory.readFrom(response.body().asString())
                            .asObject()
                            .getValue(Connection.JsonFields.ID)
                            .orElseThrow();
                    return awaitLiveStatusOrTimeout(solution, connectionId, desiredStatus)
                            .thenApply(v -> response);
                })
                .exceptionally(error -> {
                    LOGGER.error("setup connection <{}> failed", connectionName, error);
                    if (CONNECTION_RETRY_COUNTER.incrementAndGet() <= 3) {
                        try {
                            TimeUnit.SECONDS.sleep(5L);
                            asyncCreateConnection(solution, connection);
                        } catch (InterruptedException e) {
                            LOGGER.error("Error during sleep", e);
                        }
                    }
                    LOGGER.error("Couldn't setup connection after 3 attempts", error);
                    return null;
                });
    }

    private CompletableFuture<Void> awaitLiveStatusOrTimeout(final Solution solution,
            final CharSequence connectionId, final ConnectivityStatus expectedStatus) {
        return CompletableFuture.runAsync(
                () -> {
                    Awaitility.await()
                            .pollInterval(Duration.ofSeconds(1))
                            .timeout(LIVE_STATUS_POLL_TIMEOUT).untilAsserted(
                                    () -> {
                                        final ConnectivityStatus connectivityStatus =
                                                getLiveStatusOfConnection(connectionId);
                                        LOGGER.info("Connectivity status of connection <{}> is {} (expected {}).",
                                                connectionId, connectivityStatus, expectedStatus);
                                        assertThat((CharSequence) connectivityStatus).isEqualTo(expectedStatus);
                                    });
                });
    }

    public ConnectivityStatus getLiveStatusOfConnection(final CharSequence connectionId) {
        final Response statusResponse = ConnectionsClient.getInstance()
                .getConnectionStatus(connectionId)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        return RetrieveConnectionStatusResponse.fromJson(statusResponse.asString(), DittoHeaders.empty())
                .getLiveStatus();
    }

    private Connection appendDefaultHeaderMappingAndReplyTarget(final Connection connection) {
        if (!defaultHeaderMapping.isEmpty()) {
            return connection.toBuilder()
                    .setSources(connection.getSources().stream()
                            .map(s -> ConnectivityModelFactory.newSourceBuilder(s)
                                    .headerMapping(augmentHeaderMapping(s.getHeaderMapping()))
                                    .replyTarget(Optional.ofNullable(
                                                    getReplyTargetAddress.get(connection.getId().toString()))
                                            .map(address -> ReplyTarget.newBuilder()
                                                    .address(address)
                                                    .headerMapping(augmentHeaderMapping(s.getReplyTarget()
                                                            .map(ReplyTarget::getHeaderMapping)
                                                            .orElse(null)))
                                                    .expectedResponseTypes(s.getReplyTarget()
                                                            .map(ReplyTarget::getExpectedResponseTypes)
                                                            .orElse(Collections.emptySet()))
                                                    .build())
                                            .orElse(null))
                                    .build())
                            .collect(Collectors.toList()))
                    .setTargets(connection.getTargets().stream()
                            .map(t -> ConnectivityModelFactory.newTargetBuilder(t)
                                    .headerMapping(augmentHeaderMapping(t.getHeaderMapping()))
                                    .build())
                            .collect(Collectors.toList()))
                    .build();
        } else {
            return connection;
        }
    }

    private HeaderMapping augmentHeaderMapping(@Nullable final HeaderMapping mapping) {
        if (mapping == null && defaultHeaderMapping.isEmpty()) {
            return null;
        } else if (mapping == null) {
            return ConnectivityModelFactory.newHeaderMapping(defaultHeaderMapping);
        } else {
            final Map<String, String> combinedMapping = new HashMap<>();
            combinedMapping.putAll(defaultHeaderMapping);
            combinedMapping.putAll(mapping.getMapping());
            return ConnectivityModelFactory.newHeaderMapping(combinedMapping);
        }
    }

    String defaultTargetAddress(final String connectionId) {
        return getTargetAddress.get(connectionId);
    }

    String defaultSourceAddress(final String connectionId) {
        return getSourceAddress.get(connectionId);
    }

    private String getConnectionUri() {
        return getConnectionUri.get();
    }

    private Map<String, String> getSpecificConfig() {
        return getSpecificConfig.get();
    }

    @FunctionalInterface
    public interface GetConnectionUri {

        String get(final boolean tunnel, final boolean basicAuth);

        default String get() {
            return get(false, true);
        }
    }

    @FunctionalInterface
    public interface GetSpecificConfig {

        @Nullable
        Map<String, String> get();
    }

    @FunctionalInterface
    public interface GetTargetAddress {

        String get(final String connectionName);
    }

    @FunctionalInterface
    public interface GetSourceAddress {

        String get(final String connectionName);
    }

    @FunctionalInterface
    public interface GetReplyTargetAddress {

        String get(final String connectionName);
    }

    @FunctionalInterface
    public interface GetEnforcement {

        Enforcement get(final String connectionName);
    }

    @FunctionalInterface
    public interface SolutionSupplier {

        Solution getSolution();
    }

    @FunctionalInterface
    public interface GetSshTunnel {

        SshTunnel getSshTunnel();
    }

    public String disambiguate(final String prefix) {
        return String.format("%s_%X", prefix, DISAMBIGUATION_COUNTER.getAndIncrement());
    }

    public String disambiguateConnectionName(final String username, final String prefix) {
        return String.format("%s_%s_%X", username, prefix, DISAMBIGUATION_COUNTER.getAndIncrement());
    }

}
