/*
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
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
package org.eclipse.ditto.testing.common.client.oauth;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.testing.common.config.TestConfig;

/**
 * Configuration properties for Suite Auth service.
 */
@Immutable
public final class AuthConfig {

    private static final String OAUTH_PREFIX = "oauth.";
    private static final String OAUTH_TOKEN_ENDPOINT = OAUTH_PREFIX + "tokenEndpoint";
    private static final String OAUTH_AUTHORIZE_ENDPOINT = OAUTH_PREFIX + "authorizeEndpoint";
    private static final String OAUTH_CLIENT_ID = OAUTH_PREFIX + "clientId";
    private static final String OAUTH_CLIENT_SECRET = OAUTH_PREFIX + "clientSecret";
    private static final String OAUTH_CLIENT_SCOPE = OAUTH_PREFIX + "clientScope";

    private final URI tokenEndpointUri;
    private final String clientId;
    private final String clientSecret;
    private final String clientScope;

    private AuthConfig(final TestConfig config) {
        tokenEndpointUri = config.getUriOrThrow(OAUTH_TOKEN_ENDPOINT);
        clientId = config.getStringOrThrow(OAUTH_CLIENT_ID);
        clientSecret = config.getStringOrThrow(OAUTH_CLIENT_SECRET);
        clientScope = config.getStringOrThrow(OAUTH_CLIENT_SCOPE);
    }

    /**
     * Returns an instance of {@code SuiteAuthConfig}.
     *
     * @param testConfig provides the raw testConfig properties.
     * @return the instance.
     * @throws NullPointerException if {@code testConfig} is {@code null}.
     * @throws org.eclipse.ditto.testing.common.config.ConfigError if any config property is either missing or has an
     * inappropriate value.
     */
    public static AuthConfig of(final TestConfig testConfig) {
        return new AuthConfig(ConditionChecker.checkNotNull(testConfig, "testConfig"));
    }

    public URI getTokenEndpointUri() {
        return tokenEndpointUri;
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public String getClientScope() {
        return clientScope;
    }

}
