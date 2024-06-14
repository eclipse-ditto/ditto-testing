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

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.jwt.model.JsonWebToken;
import org.eclipse.ditto.testing.common.client.BasicAuth;

/**
 * Provides {@link AuthenticationSetter}s for various authentication schemes.
 */
@Immutable
public final class AuthenticationSetters {

    private AuthenticationSetters() {
        throw new AssertionError();
    }

    /**
     * Gets an {@link AuthenticationSetter} that uses the OAuth2 access (bearer) token which is supplied by the
     * specified argument for authenticating requests preemptively.
     * The access token will be put in the request headers.
     *
     * @param jsonWebTokenSupplier supplies the access token to be used for preemptive OAuth 2 authentication.
     * @return the {@code AuthenticationSetter}.
     * @throws NullPointerException if {@code jsonWebTokenSupplier} is {@code null}.
     */
    public static AuthenticationSetter oauth2AccessToken(final AccessTokenSupplier<JsonWebToken> jsonWebTokenSupplier) {
        ConditionChecker.checkNotNull(jsonWebTokenSupplier, "jsonWebTokenSupplier");
        return authSpec -> {
            final var jsonWebToken = jsonWebTokenSupplier.getAccessToken();
            return authSpec.preemptive().oauth2(jsonWebToken.getToken());
        };
    }

    /**
     * Gets an {@link AuthenticationSetter} that uses "devops" username and password as
     * preemptive basic auth authentication.
     *
     * @return the {@code AuthenticationSetter}.
     */
    public static AuthenticationSetter devopsAuthentication() {
        return authSpec -> authSpec.preemptive().basic("devops", "foobar");
    }

    /**
     * Gets an {@link AuthenticationSetter} that uses "ditto" username and password as
     * preemptive basic auth authentication.
     *
     * @return the {@code AuthenticationSetter}.
     */
    public static AuthenticationSetter basicAuthentication(BasicAuth basicAuth) {
        return authSpec -> authSpec.preemptive().basic(basicAuth.getUsername(), basicAuth.getPassword());
    }

}
