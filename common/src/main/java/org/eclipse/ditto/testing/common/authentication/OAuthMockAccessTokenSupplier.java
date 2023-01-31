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

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.jwt.model.JsonWebToken;
import org.eclipse.ditto.testing.common.Solution;

/**
 * Supplies the access token that is issued by the {@link OAuthMockClient}.
 */
@Immutable
final class OAuthMockAccessTokenSupplier implements AccessTokenSupplier<JsonWebToken> {

    private final OAuthMockClient oAuthMockClient;
    private final ClientCredentials clientCredentials;

    private OAuthMockAccessTokenSupplier(final OAuthMockClient oAuthMockClient,
            final ClientCredentials clientCredentials) {

        this.oAuthMockClient = oAuthMockClient;
        this.clientCredentials = clientCredentials;
    }

    static OAuthMockAccessTokenSupplier newInstance(final OAuthMockConfig oAuthMockConfig,
            final Solution solution) {

        final var oAuthMockClient = getOAuthMockClient(oAuthMockConfig);
        final var clientCredentials = ClientCredentials.of(solution.getUsername(), "someSecret");

        return new OAuthMockAccessTokenSupplier(oAuthMockClient, clientCredentials);
    }

    private static OAuthMockClient getOAuthMockClient(final OAuthMockConfig oAuthMockConfig) {
        return OAuthMockClient.newInstance(oAuthMockConfig.getTokenEndpointUri());
    }

    @Override
    public JsonWebToken getAccessToken() {
        return oAuthMockClient.getAccessToken(clientCredentials, "system-test");
    }

}
