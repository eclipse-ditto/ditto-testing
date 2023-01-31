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
package org.eclipse.ditto.testing.system.connectivity.amqp10;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;

import javax.jms.Message;

import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionType;
import org.eclipse.ditto.connectivity.model.HmacCredentials;
import org.eclipse.ditto.connectivity.model.UserPasswordCredentials;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.testing.common.Solution;
import org.eclipse.ditto.testing.common.conditions.DockerEnvironment;
import org.eclipse.ditto.testing.common.conditions.RunIf;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityITCommon;
import org.eclipse.ditto.testing.system.connectivity.AbstractConnectivityWorker;
import org.eclipse.ditto.testing.system.connectivity.ConnectionCategory;
import org.eclipse.ditto.testing.system.connectivity.Connections;
import org.eclipse.ditto.testing.system.connectivity.ConnectivityFactory;
import org.eclipse.ditto.testing.system.connectivity.httppush.AzSaslRequestSigning;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for SASL layer of AMQP 1.0 connections.
 */
@RunIf(DockerEnvironment.class)
public class Amqp10SaslIT extends AbstractConnectivityITCommon<BlockingQueue<Message>, Message> {

    private Amqp10TestServer amqp10Server;

    public Amqp10SaslIT() {
        super(ConnectivityFactory.of(
                "Amqp10Sasl",
                connectionModelFactory,
                () -> SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution(),
                ConnectionType.AMQP_10,
                Amqp10SaslIT::getAmqpUri,
                () -> Collections.singletonMap("jms.closeTimeout", "0"),
                Amqp10SaslIT::defaultTargetAddress,
                Amqp10SaslIT::defaultSourceAddress,
                id -> null,
                () -> null,
                SOLUTION_CONTEXT_WITH_RANDOM_NS.getOAuthClient()));
    }

    @Override
    protected AbstractConnectivityWorker<BlockingQueue<Message>, Message> getConnectivityWorker() {
        throw new UnsupportedOperationException();
    }

    @Before
    public void setupConnectivity() throws Exception {
        amqp10Server = new Amqp10TestServer(CONFIG.getAmqp10HonoPort());
        amqp10Server.startServer();
    }

    @After
    public void cleanupConnectivity() throws Exception {
        try {
            cleanupConnections(SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution().getUsername());
        } finally {
            amqp10Server.stopServer();
            amqp10Server = null;
        }
    }

    @Test
    @Connections(ConnectionCategory.NONE)
    public void azSaslPlain() {
        // WHEN: An open AMQP connection is created with HMAC credentials with the algorithm "az-sasl".
        final Solution solution = SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution();
        final String connectionName = cf.disambiguateConnectionName(
                SOLUTION_CONTEXT_WITH_RANDOM_NS.getSolution().getUsername(), "AmqpWithAzSaslAuth");
        final String skn = "RootSharedAccessKeyPolicy";
        final String endpointWithoutAzureSuffix = "localhost";
        final String endpoint = endpointWithoutAzureSuffix + ".azure-devices.net";

        final String sharedKey = "SGFsbG8gV2VsdCEgSXN0IGRhcyBhbG";
        final HmacCredentials credentials = HmacCredentials.of("az-sasl", JsonObject.newBuilder()
                .set("sharedKeyName", skn)
                .set("sharedKey", sharedKey)
                .set("endpoint", endpoint)
                .build());

        final Connection singleConnectionWithAzureSaslAuth = cf.getSingleConnection(solution.getUsername(), connectionName)
                .toBuilder()
                .uri(getAmqpUri(false, false))
                .credentials(credentials)
                .build();
        cf.asyncCreateConnection(solution, singleConnectionWithAzureSaslAuth).join();

        // THEN: 1 SASL-PLAIN authentication attempt was made.
        final var authAttempts = amqp10Server.getMockSaslAuthenticatorFactory().getAuthAttempts();
        assertThat(authAttempts.size())
                .describedAs("Expect exactly 1 authentication attempt, got: %s", authAttempts)
                .isEqualTo(1);
        final var saslPlainCredentials = authAttempts.element();

        final AzSaslRequestSigning saslRequestSigning = AzSaslRequestSigning.newInstance(endpoint, skn, sharedKey);
        final UserPasswordCredentials
                expectedCredentials = saslRequestSigning.generateSignedUserCredentials(skn, endpointWithoutAzureSuffix,
                saslPlainCredentials);

        // THEN: The authorization header of the HTTP request conforms to Azure-SAS pattern.
        assertThat(saslPlainCredentials).isEqualTo(expectedCredentials);
    }

    private static String defaultSourceAddress(final String suffix) {
        return "source_" + suffix;
    }

    private static String defaultTargetAddress(final String suffix) {
        return "target_" + suffix;
    }

    @Override
    protected String targetAddressForTargetPlaceHolderSubstitution() {
        return "target_address_for_target_placeholder_substitution";
    }

    private static String getAmqpUri(final boolean tunnel, final boolean basicAuth) {
        final String host = tunnel ? CONFIG.getAmqp10HonoTunnel() : CONFIG.getAmqp10HonoHostName();
        return "amqp://" + host + ":" + CONFIG.getAmqp10HonoPort();
    }
}
