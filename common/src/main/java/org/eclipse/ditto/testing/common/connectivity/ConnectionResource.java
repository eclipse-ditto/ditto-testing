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
package org.eclipse.ditto.testing.common.connectivity;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.text.MessageFormat;
import java.time.Duration;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.awaitility.Awaitility;
import org.awaitility.Durations;
import org.eclipse.ditto.connectivity.model.Connection;
import org.eclipse.ditto.connectivity.model.ConnectionId;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.ConnectionsHttpClient;
import org.eclipse.ditto.testing.common.config.TestEnvironment;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.junit.ExternalResourceState;
import org.junit.rules.ExternalResource;

/**
 * This external resource creates a Connection for a solution when a test is run.
 * After the test, the created Connection will be deleted.
 */
@NotThreadSafe
public final class ConnectionResource extends ExternalResource {

    private final TestEnvironment testEnvironment;
    private final Supplier<ConnectionsHttpClient> connectionsHttpClientSupplier;
    private final Supplier<Connection> connectionSupplier;

    private ExternalResourceState state;
    private ConnectionsHttpClient connectionsHttpClient;
    private Connection connection;

    private ConnectionResource(final TestEnvironment testEnvironment,
            final Supplier<ConnectionsHttpClient> connectionsHttpClientSupplier,
            final Supplier<Connection> connectionSupplier
    ) {

        this.testEnvironment = testEnvironment;
        this.connectionSupplier = connectionSupplier;
        this.connectionsHttpClientSupplier = connectionsHttpClientSupplier;

        state = ExternalResourceState.INITIAL;
        connectionsHttpClient = null;
        connection = null;
    }

    /**
     * Returns a new instance of {@code ConnectionResource} for the specified arguments.
     *
     * @param testEnvironment the environment where the test is executed.
     * @param connectionSupplier supplies the Connection to be created.
     * @return the new instance.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public static ConnectionResource newInstance(final TestEnvironment testEnvironment,
            final Supplier<ConnectionsHttpClient> connectionsHttpClientSupplier,
            final Supplier<Connection> connectionSupplier) {

        return new ConnectionResource(checkNotNull(testEnvironment, "testEnvironment"),
                connectionsHttpClientSupplier,
                checkNotNull(connectionSupplier, "connectionSupplier"));
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        final var baseCorrelationId = CorrelationId.random();
        connection = postConnection(getRequestConnectionOrThrow(),
                baseCorrelationId.withSuffix(".postConnection"));
        waitUntilConnectionLiveStatusIsOpen(baseCorrelationId.withSuffix(".fetchLiveStatusOpen"));
        state = ExternalResourceState.POST_BEFORE;
    }

    private Connection getRequestConnectionOrThrow() {
        return checkNotNull(connectionSupplier.get(), "supplied connection");
    }

    private Connection postConnection(final Connection connection, final CorrelationId correlationId) {
        return getConnectionsHttpClient().postConnection(connection, correlationId);
    }

    private void waitUntilConnectionLiveStatusIsOpen(final CorrelationId correlationId) {
        Awaitility.await()
                .pollDelay(Durations.TWO_SECONDS)
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Durations.ONE_SECOND)
                .until(() -> isConnectionLiveStatusOpen(correlationId));
    }

    private boolean isConnectionLiveStatusOpen(final CorrelationId correlationId) {
        final var connectionStatus = getConnectionsHttpClient()
                .getConnectionStatus(connection.getId(), correlationId);

        return connectionStatus.getValue("liveStatus")
                .filter(JsonValue::isString)
                .map(JsonValue::asString)
                .filter("open"::equals)
                .isPresent();
    }

    @Override
    protected void after() {
        if (TestEnvironment.LOCAL == testEnvironment || TestEnvironment.DOCKER_COMPOSE == testEnvironment) {
            deleteConnectionFromTestSolution();
        }
        connection = null;
        super.after();
        state = ExternalResourceState.POST_AFTER;
    }

    private ConnectionsHttpClient getConnectionsHttpClient() {
        var result = connectionsHttpClient;
        if (null == result) {
            result = connectionsHttpClientSupplier.get();
            connectionsHttpClient = result;
        }
        return result;
    }

    private void deleteConnectionFromTestSolution() {
        getConnectionsHttpClient().deleteConnection(connection.getId(), CorrelationId.random());
    }

    public Connection getConnection() {
        if (ExternalResourceState.POST_BEFORE == state) {
            return connection;
        } else {
            final var connectionType = connection.getConnectionType();
            throw new IllegalStateException(
                    MessageFormat.format("The {} connection was either not yet initialised or got already discarded.",
                            connectionType.getName())
            );
        }
    }

    public ConnectionId getConnectionId() {
        return getConnection().getId();
    }

}
