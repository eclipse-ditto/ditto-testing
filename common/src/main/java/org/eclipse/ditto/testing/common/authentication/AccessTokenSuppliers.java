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
import org.eclipse.ditto.jwt.model.JsonWebToken;
import org.eclipse.ditto.testing.common.Solution;

/**
 * Provides various {@link AccessTokenSupplier}s.
 */
@Immutable
public final class AccessTokenSuppliers {

    private AccessTokenSuppliers() {
        throw new AssertionError();
    }

    public static AccessTokenSupplier<JsonWebToken> getCachingSuiteAuthMockJwtSupplier(
            final OAuthMockConfig oAuthMockConfig,
            final Solution solution) {
        return CachingJwtSupplier.of(OAuthMockAccessTokenSupplier.newInstance(oAuthMockConfig, solution));
    }

    public static AccessTokenSupplier<JsonWebToken> getCachingSuiteAuthJwtSupplier(final URI suiteAuthTokenEndpointUri,
            final CharSequence clientId,
            final CharSequence clientSecret,
            final CharSequence clientScope) {

        ConditionChecker.argumentNotEmpty(clientScope, "clientScope");

        final var suiteAuthClient = OAuthClient.newInstance(suiteAuthTokenEndpointUri);
        final var clientCredentials = ClientCredentials.of(clientId, clientSecret);

        return CachingJwtSupplier.of(() -> suiteAuthClient.getAccessToken(clientCredentials, clientScope));
    }

    // Add here missing suppliers as required.

}
