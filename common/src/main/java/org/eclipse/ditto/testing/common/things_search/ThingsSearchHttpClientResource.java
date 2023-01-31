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
package org.eclipse.ditto.testing.common.things_search;

import java.net.URI;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.jwt.model.JsonWebToken;
import org.eclipse.ditto.testing.common.authentication.AccessTokenSupplier;
import org.eclipse.ditto.testing.common.authentication.AuthenticationSetter;
import org.eclipse.ditto.testing.common.authentication.AuthenticationSetters;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.gateway.GatewayConfig;
import org.junit.rules.ExternalResource;

/**
 * This external resource creates and provides a {@link ThingsSearchHttpClient}.
 */
@NotThreadSafe
public final class ThingsSearchHttpClientResource extends ExternalResource {

    private final URI httpApiUri;
    private final AuthenticationSetter authenticationSetter;

    private ThingsSearchHttpClient thingsSearchClient;

    private ThingsSearchHttpClientResource(final URI httpApiUri,
            final AuthenticationSetter authenticationSetter) {

        this.httpApiUri = httpApiUri;
        this.authenticationSetter = authenticationSetter;

        thingsSearchClient = null;
    }

    public static ThingsSearchHttpClientResource newInstance(final TestConfig testConfig,
            final AccessTokenSupplier<JsonWebToken> jwtSupplier) {

        final var gatewayConfig = GatewayConfig.of(testConfig);
        return new ThingsSearchHttpClientResource(gatewayConfig.getHttpUriApi2(),
                AuthenticationSetters.oauth2AccessToken(jwtSupplier));
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        thingsSearchClient = ThingsSearchHttpClient.newInstance(httpApiUri, authenticationSetter);
    }

    public ThingsSearchHttpClient getThingsSearchClient() {
        if (null == thingsSearchClient) {
            throw new IllegalStateException("The things search client gets only initialised by running the test.");
        } else {
            return thingsSearchClient;
        }
    }

}
