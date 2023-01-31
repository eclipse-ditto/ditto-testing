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
package org.eclipse.ditto.testing.common.authentication;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.testing.common.config.TestConfig;

/**
 * Configuration properties for our OAuth mock service.
 */
@Immutable
public final class OAuthMockConfig {

    private static final String OAUTH_MOCK_PREFIX = "oauth-mock.";
    private static final String OAUTH_MOCK_TOKEN_ENDPOINT = OAUTH_MOCK_PREFIX + "tokenEndpoint";

    private final URI oAuthMockTokenEndpointUri;

    private OAuthMockConfig(final TestConfig config) {
        oAuthMockTokenEndpointUri = config.getUriOrThrow(OAUTH_MOCK_TOKEN_ENDPOINT);
    }

    /**
     * Returns an instance of {@code OAuthMockConfig}.
     *
     * @param testConfig provides the raw testConfig properties.
     * @return the instance.
     * @throws NullPointerException if {@code testConfig} is {@code null}.
     * @throws org.eclipse.ditto.testing.common.config.ConfigError if any config property is either missing or has an
     * inappropriate value.
     */
    public static OAuthMockConfig of(final TestConfig testConfig) {
        return new OAuthMockConfig(ConditionChecker.checkNotNull(testConfig, "testConfig"));
    }

    public URI getTokenEndpointUri() {
        return oAuthMockTokenEndpointUri;
    }

}
