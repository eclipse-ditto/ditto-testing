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
import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.CONNECTION_WITH_SSH_TUNNEL;
import static org.eclipse.ditto.testing.system.connectivity.ConnectionCategory.NONE;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.json.Jsonifiable;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.ConnectivityStatus;
import org.eclipse.ditto.connectivity.model.FilteredTopic;
import org.eclipse.ditto.connectivity.model.SshPublicKeyCredentials;
import org.eclipse.ditto.connectivity.model.SshTunnel;
import org.eclipse.ditto.connectivity.model.Target;
import org.eclipse.ditto.connectivity.model.Topic;
import org.eclipse.ditto.connectivity.model.UserPasswordCredentials;
import org.eclipse.ditto.connectivity.model.signals.announcements.ConnectionClosedAnnouncement;
import org.eclipse.ditto.connectivity.model.signals.announcements.ConnectionOpenedAnnouncement;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.protocol.Adaptable;
import org.eclipse.ditto.protocol.JsonifiableAdaptable;
import org.eclipse.ditto.protocol.ProtocolFactory;
import org.eclipse.ditto.testing.common.categories.RequireSource;
import org.eclipse.ditto.testing.common.categories.RequireSshTunnel;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.signals.events.ThingCreated;
import org.junit.Test;
import org.junit.experimental.categories.Category;

public abstract class AbstractConnectivityTunnelingITestCases<C, M>
        extends AbstractConnectivityPayloadMappingITestCases<C, M> {

    private static final String PUBLIC_KEY_AUTHENTICATION_MOD = "PUBLIC_KEY_AUTHENTICATION";
    private static final String WRONG_PLAIN_CREDENTIALS = "WRONG_PLAIN_CREDENTIALS";
    private static final String ENDPOINT_NOT_REACHABLE = "ENDPOINT_NOT_REACHABLE";
    private static final String INVALID_FINGERPRINT = "INVALID_FINGERPRINT";
    private static final String CONNECTION_ANNOUNCEMENTS = "CONNECTION_ANNOUNCEMENTS";

    static {
        // replaces the default plain authentication with public key authentication
        addMod(PUBLIC_KEY_AUTHENTICATION_MOD, connection -> {
                    final SshTunnel sshTunnelWithPublicKeyAuthentication =
                            connection.getSshTunnel()
                                    .map(ConnectivityModelFactory::newSshTunnelBuilder)
                                    .orElseThrow()
                                    .credentials(SshPublicKeyCredentials.of(CONFIG.getSshUsername(),
                                            CONFIG.getTunnelPublicKey(), CONFIG.getTunnelPrivateKey()))
                                    .build();
                    return connection.toBuilder().sshTunnel(sshTunnelWithPublicKeyAuthentication).build();
                }
        );
        // replaces the default plain authentication invalid ones
        addMod(WRONG_PLAIN_CREDENTIALS, connection -> {
                    final SshTunnel sshTunnelWithInvalidCredentials =
                            connection.getSshTunnel()
                                    .map(ConnectivityModelFactory::newSshTunnelBuilder)
                                    .orElseThrow()
                                    .credentials(UserPasswordCredentials.newInstance("invalid", "invalid"))
                                    .build();
                    return connection.toBuilder()
                            .connectionStatus(ConnectivityStatus.CLOSED)
                            .sshTunnel(sshTunnelWithInvalidCredentials)
                            .build();
                }
        );
        // unreachable endpoint
        addMod(ENDPOINT_NOT_REACHABLE, connection -> {
                    final SshTunnel sshTunnelWithPublicKeyAuthentication =
                            connection.getSshTunnel()
                                    .map(ConnectivityModelFactory::newSshTunnelBuilder)
                                    .orElseThrow()
                                    .uri("ssh://unknown:22")
                                    .build();
                    return connection.toBuilder()
                            .connectionStatus(ConnectivityStatus.CLOSED)
                            .sshTunnel(sshTunnelWithPublicKeyAuthentication)
                            .build();
                }
        );
        addMod(INVALID_FINGERPRINT, connection -> {
                    final SshTunnel sshTunnelWithInvalidFingerprint =
                            connection.getSshTunnel()
                                    .map(ConnectivityModelFactory::newSshTunnelBuilder)
                                    .orElseThrow()
                                    .validateHost(true)
                                    .knownHosts(List.of("MD5:42:83:ff:54:d1:d2:7c:f7:fa:ea:e3:33:73:0c:6a:24"))
                                    .build();
                    return connection.toBuilder()
                            .connectionStatus(ConnectivityStatus.CLOSED)
                            .sshTunnel(sshTunnelWithInvalidFingerprint)
                            .build();
                }
        );
        addMod(CONNECTION_ANNOUNCEMENTS, connection -> {
                    final List<Target> adjustedTargets =
                            connection.getTargets().stream()
                                    .map(target -> {
                                        final Set<FilteredTopic> topics = new HashSet<>(target.getTopics());
                                        topics.add(ConnectivityModelFactory
                                                .newFilteredTopicBuilder(Topic.CONNECTION_ANNOUNCEMENTS)
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
    }

    protected AbstractConnectivityTunnelingITestCases(final ConnectivityFactory cf) {
        super(cf);
    }

    @Test
    @Category({RequireSource.class, RequireSshTunnel.class})
    @Connections(CONNECTION_WITH_SSH_TUNNEL)
    public void sendCreateThingViaTunnelAndEnsureResponseIsSentBack() {
        sendCreateThingAndEnsureResponseIsSentBack(cf.connectionWithTunnel);
    }

    @Test
    @Category(RequireSshTunnel.class)
    @Connections(CONNECTION_WITH_SSH_TUNNEL)
    public void createThingViaHttpAndExpectEventIsPublishedViaTunnel() {
        final String correlationId = UUID.randomUUID().toString();

        final ThingId thingId = generateThingId(randomNamespace);
        final Policy policy = Policy.newBuilder()
                .forLabel("DEFAULT")
                .setSubject(testingContextWithRandomNs.getOAuthClient().getDefaultSubject())
                .setSubject(connectionSubject(cf.connectionWithTunnel))
                .setGrantedPermissions(PoliciesResourceType.thingResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.policyResource("/"), READ, WRITE)
                .setGrantedPermissions(PoliciesResourceType.messageResource("/"), READ, WRITE)
                .build();

        final C eventConsumer = initTargetsConsumer(cf.connectionWithTunnel);

        putThingWithPolicy(2, Thing.newBuilder().setId(thingId).build(), policy, JsonSchemaVersion.V_2)
                .withJWT(testingContextWithRandomNs.getOAuthClient().getAccessToken())
                .withCorrelationId(correlationId)
                .expectingHttpStatus(CREATED)
                .fire();

        consumeAndAssertEvents(cf.connectionWithTunnel, eventConsumer, Collections.singletonList(
                e -> {
                    final ThingCreated tc = thingEventForJson(e, ThingCreated.class, correlationId, thingId);
                    assertThat(tc.getRevision()).isEqualTo(1L);
                }), "ThingCreated");

    }

    @Test
    @Category({RequireSource.class, RequireSshTunnel.class})
    @UseConnection(category = CONNECTION_WITH_SSH_TUNNEL, mod = PUBLIC_KEY_AUTHENTICATION_MOD)
    public void sendCreateThingViaTunnelAndEnsureResponseIsSentBackWithPublicKeyAuth() {
        sendCreateThingAndEnsureResponseIsSentBack(cf.connectionWithTunnel);
    }

    @Test
    @Category(RequireSshTunnel.class)
    @UseConnection(category = CONNECTION_WITH_SSH_TUNNEL, mod = WRONG_PLAIN_CREDENTIALS)
    public void useInvalidPlainCredentialsForSshTunnel() {

        final ConnectionId connectionId = cf.getConnectionId(cf.connectionWithTunnel);

        connectionsClient().openConnection(connectionId.toString())
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.GATEWAY_TIMEOUT)
                .expectingErrorCode("connectivity:connection.failed")
                .fire();
    }

    @Test
    @Category(RequireSshTunnel.class)
    @UseConnection(category = CONNECTION_WITH_SSH_TUNNEL, mod = ENDPOINT_NOT_REACHABLE)
    public void sshEndpointNotReachable() {

        final ConnectionId connectionId = cf.getConnectionId(cf.connectionWithTunnel);

        connectionsClient().openConnection(connectionId.toString())
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.GATEWAY_TIMEOUT)
                .expectingErrorCode("connectivity:connection.failed")
                .fire();
    }

    @Test
    @Category(RequireSshTunnel.class)
    @UseConnection(category = CONNECTION_WITH_SSH_TUNNEL, mod = INVALID_FINGERPRINT)
    public void invalidFingerprintForSshEndpoint() {

        final ConnectionId connectionId = cf.getConnectionId(cf.connectionWithTunnel);

        connectionsClient().openConnection(connectionId.toString())
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.GATEWAY_TIMEOUT)
                .expectingErrorCode("connectivity:connection.failed")
                .fire();
    }

    @Test
    @Category(RequireSshTunnel.class)
    @UseConnection(category = CONNECTION_WITH_SSH_TUNNEL, mod = CONNECTION_ANNOUNCEMENTS)
    public void connectionWithSshTunnelAndConnectionAnnouncements() {

        final String connectionName = cf.connectionWithTunnel;
        final String connectionId = cf.getConnectionId(connectionName).toString();

        final C consumer = initTargetsConsumer(connectionName);

        // some connection types will still receive the opened announcement after initializing the targets consumer,
        // some targets will already have received it before initializing the target in this test. Therefore they
        // either receive an opened announcement or nothing
        expectMsgClassOrNothing(ConnectionOpenedAnnouncement.class, connectionName, consumer);

        connectionsClient().closeConnection(connectionId)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final M connectionClosedAnnouncementMessage = consumeFromTarget(connectionName, consumer);
        assertThat(connectionClosedAnnouncementMessage)
                .withFailMessage("Did not receive a connection closed announcement.")
                .isNotNull();

        final JsonifiableAdaptable jsonifiableAdaptable = jsonifiableAdaptableFrom(connectionClosedAnnouncementMessage);
        final Adaptable eventAdaptable = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable).build();
        final Jsonifiable<JsonObject> event = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable);
        assertThat(event).isInstanceOf(ConnectionClosedAnnouncement.class);

        connectionsClient().openConnection(connectionId)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final M connectionOpenedAnnouncementMessage = consumeFromTarget(connectionName, consumer);
        assertThat(connectionOpenedAnnouncementMessage)
                .withFailMessage("Did not receive a connection opened announcement.")
                .isNotNull();

        final JsonifiableAdaptable jsonifiableAdaptable2 = jsonifiableAdaptableFrom(connectionOpenedAnnouncementMessage);
        final Adaptable eventAdaptable2 = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable2).build();
        final Jsonifiable<JsonObject> event2 = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable2);
        assertThat(event2).isInstanceOf(ConnectionOpenedAnnouncement.class);

        connectionsClient().deleteConnection(connectionId)
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // TODO connection closed announcements after deletion do not yet work
//        final M connectionCloseAnnouncementMessage2 = consumeFromTarget(connectionName, consumer);
//        assertThat(connectionCloseAnnouncementMessage2)
//                .withFailMessage("Did not receive a connection close announcement after deletion.")
//                .isNotNull();
//
//        final JsonifiableAdaptable jsonifiableAdaptable3 = jsonifiableAdaptableFrom(connectionCloseAnnouncementMessage2);
//        final Adaptable eventAdaptable3 = ProtocolFactory.newAdaptableBuilder(jsonifiableAdaptable3).build();
//        final Jsonifiable<JsonObject> event3 = PROTOCOL_ADAPTER.fromAdaptable(eventAdaptable3);
//        assertThat(event3).isInstanceOf(ConnectionOpenedAnnouncement.class);
    }

    @Test
    @Category(RequireSshTunnel.class)
    @Connections(NONE) // connection is sent via testConnection command
    public void testConnectionWithSshTunnel() {

        final org.eclipse.ditto.connectivity.model.Connection connection =
                cf.getSingleConnectionWithTunnel("testConnectionWithSshTunnel")
                        .toBuilder()
                        .id(ConnectionId.of("testConnectionWithTunnel")) // to avoid exception because of too long ack label in target
                        .build();

        connectionsClient().testConnection(connection.toJson())
                .withDevopsAuth()
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    protected final ThingId sendCreateThingAndEnsureResponseIsSentBack(final String connectionName) {
        return sendCreateThingAndEnsureResponseIsSentBack(connectionName, ThingBuilder.FromScratch::build);
    }


}
