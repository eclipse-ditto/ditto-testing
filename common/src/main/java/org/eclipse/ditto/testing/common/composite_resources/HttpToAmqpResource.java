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
package org.eclipse.ditto.testing.common.composite_resources;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.eclipse.ditto.base.model.auth.AuthorizationContext;
import org.eclipse.ditto.base.model.auth.AuthorizationSubject;
import org.eclipse.ditto.base.model.auth.DittoAuthorizationContextType;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.ConnectivityModelFactory;
import org.eclipse.ditto.connectivity.model.ConnectivityStatus;
import org.eclipse.ditto.connectivity.model.Topic;
import org.eclipse.ditto.testing.common.ConnectionsHttpClientResource;
import org.eclipse.ditto.testing.common.TestSolutionResource;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.connectivity.ConnectionResource;
import org.eclipse.ditto.testing.common.connectivity.amqp.AmqpClientResource;
import org.eclipse.ditto.testing.common.connectivity.amqp.AmqpConfig;
import org.eclipse.ditto.testing.common.policies.PoliciesHttpClientResource;
import org.eclipse.ditto.testing.common.things.ThingsHttpClientResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Composite resource consists of:
 * <ul>
 *     <li>{@link ThingsHttpClientResource}</li>
 *     <li>{@link PoliciesHttpClientResource}</li>
 *     <li>{@link AmqpClientResource}</li>
 *     <li>{@link org.eclipse.ditto.testing.common.connectivity.ConnectionResource}</li>
 * </ul>
 * <p>
 * Provides all necessary tools/resources to build test-scenarios where the HTTP-API is used in combination
 * with one managed AMQP connection.
 */
public final class HttpToAmqpResource implements TestRule {

    private final TestSolutionResource testSolutionResource;
    private final ThingsHttpClientResource thingsHttpClientResource;
    private final PoliciesHttpClientResource policiesHttpClientResource;
    private final ConnectionsHttpClientResource connectionsHttpClientResource;
    private final AmqpClientResource amqpClientResource;
    private final ConnectionResource amqpConnectionResource;

    private HttpToAmqpResource(final TestConfig testConfig,
            final TestSolutionResource testSolutionResource,
            final ConnectionsHttpClientResource connectionsHttpClientResource,
            final AmqpClientResource amqpClientResource,
            final ConnectionResource amqpConnectionResource) {

        this.testSolutionResource = testSolutionResource;
        thingsHttpClientResource = ThingsHttpClientResource.newInstance(testConfig, testSolutionResource);
        policiesHttpClientResource = PoliciesHttpClientResource.newInstance(testConfig, testSolutionResource);
        this.connectionsHttpClientResource = connectionsHttpClientResource;
        this.amqpClientResource = amqpClientResource;
        this.amqpConnectionResource = amqpConnectionResource;
    }

    public static HttpToAmqpResource newInstance(final TestConfig testConfig,
            final TestSolutionResource testSolutionResource) {
        return newInstance(
                testConfig,
                testSolutionResource,
                amqpClientResource -> {
                    final var authorizationContext = AuthorizationContext.newInstance(
                            DittoAuthorizationContextType.PRE_AUTHENTICATED_CONNECTION,
                            AuthorizationSubject.newInstance(
                                    MessageFormat.format("integration:{0}:{1}",
                                            testSolutionResource.getTestUsername(),
                                            amqpClientResource.getConnectionName())
                            )
                    );
                    return ConnectivityModelFactory.newConnectionBuilder(
                                    ConnectionId.generateRandom(),
                                    ConnectionType.AMQP_10,
                                    ConnectivityStatus.OPEN,
                                    amqpClientResource.getEndpointUri().toString()
                            )
                            .name(amqpClientResource.getConnectionName())
                            .specificConfig(Map.of("jms.closeTimeout", "0"))
                            .targets(List.of(ConnectivityModelFactory.newTargetBuilder()
                                    .authorizationContext(authorizationContext)
                                    .address(amqpClientResource.getTargetAddress())
                                    .topics(Topic.LIVE_COMMANDS)
                                    .qos(1)
                                    .build()))
                            .sources(List.of(ConnectivityModelFactory.newSourceBuilder()
                                    .authorizationContext(authorizationContext)
                                    .address(amqpClientResource.getSourceAddress())
                                    .replyTargetEnabled(true)
                                    .payloadMapping(ConnectivityModelFactory.newPayloadMapping(
                                            "Ditto",
                                            "UpdateTwinWithLiveResponse"
                                    ))
                                    .build()
                            ))
                            .build();
                }
        );
    }

    public static HttpToAmqpResource newInstance(
            final TestConfig testConfig,
            final TestSolutionResource testSolutionResource,
            final Function<AmqpClientResource, Connection> connectionSupplier
    ) {
        final var connectionsHttpClientResource =
                ConnectionsHttpClientResource.newInstance(testConfig);
        final var amqpClientResource = AmqpClientResource.newInstance(AmqpConfig.of(testConfig));

        return new HttpToAmqpResource(
                testConfig,
                testSolutionResource,
                connectionsHttpClientResource,
                amqpClientResource,
                ConnectionResource.newInstance(
                        testConfig.getTestEnvironment(),
                        connectionsHttpClientResource,
                        () -> connectionSupplier.apply(amqpClientResource)
                )
        );
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
        return RuleChain.outerRule(testSolutionResource)
                .around(amqpClientResource)
                .around(thingsHttpClientResource)
                .around(policiesHttpClientResource)
                .around(connectionsHttpClientResource)
                .around(amqpConnectionResource)
                .apply(base, description);
    }

    public TestSolutionResource getTestSolutionResource() {
        return testSolutionResource;
    }

    public AmqpClientResource getAmqpClientResource() {
        return amqpClientResource;
    }

    public ThingsHttpClientResource getThingsHttpClientResource() {
        return thingsHttpClientResource;
    }

    public PoliciesHttpClientResource getPoliciesHttpClientResource() {
        return policiesHttpClientResource;
    }

    public ConnectionsHttpClientResource getConnectionsHttpClientResource() {
        return connectionsHttpClientResource;
    }

    public ConnectionResource getAmqpConnectionResource() {
        return amqpConnectionResource;
    }

}
