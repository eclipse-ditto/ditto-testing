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
package org.eclipse.ditto.testing.common;

import java.net.URI;
import java.util.function.Supplier;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.testing.common.authentication.AuthenticationSetter;
import org.eclipse.ditto.testing.common.authentication.AuthenticationSetters;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.gateway.GatewayConfig;
import org.junit.rules.ExternalResource;

/**
 * This external resource creates and provides a {@link ConnectionsHttpClient}.
 */
@NotThreadSafe
public final class ConnectionsHttpClientResource extends ExternalResource implements Supplier<ConnectionsHttpClient> {

    private final URI httpApiUri;
    private final AuthenticationSetter authenticationSetter;
    private ConnectionsHttpClient connectionsClient;

    private ConnectionsHttpClientResource(final URI httpApiUri, final AuthenticationSetter authenticationSetter) {
        this.httpApiUri = httpApiUri;
        this.authenticationSetter = authenticationSetter;
    }

    public static ConnectionsHttpClientResource newInstance(final TestConfig testConfig) {

        final var gatewayConfig = GatewayConfig.of(testConfig);
        return new ConnectionsHttpClientResource(gatewayConfig.getHttpUriApi2(),
                AuthenticationSetters.devopsAuthentication());
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        connectionsClient = ConnectionsHttpClient.newInstance(httpApiUri, authenticationSetter);
    }

    public ConnectionsHttpClient getConnectionsClient() {
        if (null == connectionsClient) {
            throw new IllegalStateException("The connections client gets only initialised by running the test.");
        } else {
            return connectionsClient;
        }
    }

    @Override
    public ConnectionsHttpClient get() {
        return getConnectionsClient();
    }

}
