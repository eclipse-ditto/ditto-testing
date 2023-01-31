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

import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.ThreadSafe;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.jwt.model.JsonWebToken;

/**
 * Requests an access token as JWT from an OAuth2 authorization server in order to perform a "client credentials flow".
 * The retrieved token is cached to reduce requests to the authorization server.
 * If the cached JWT is expired a new one will be acquired automatically.
 */
@ThreadSafe
final class CachingJwtSupplier implements AccessTokenSupplier<JsonWebToken> {

    private final AccessTokenSupplier<JsonWebToken> jwtSupplier;
    private final AtomicReference<JsonWebToken> jwtHolder;

    private CachingJwtSupplier(final AccessTokenSupplier<JsonWebToken> jwtSupplier) {
        this.jwtSupplier = jwtSupplier;
        jwtHolder = new AtomicReference<>(null);
    }

    /**
     * Returns a new instance of {@code OAuth2ClientCredentialsBearerTokenProvider}.
     *
     * @param jwtSupplier the actual supplier of a {@link JsonWebToken} that is called if this class' algorithm decides
     * to.
     * @return the instance.
     * @throws NullPointerException if any argument is {@code null}.
     * @throws IllegalArgumentException if {@code scope} is empty.
     */
    static CachingJwtSupplier of(final AccessTokenSupplier<JsonWebToken> jwtSupplier) {
        return new CachingJwtSupplier(ConditionChecker.checkNotNull(jwtSupplier, "jwtSupplier"));
    }

    @Override
    public JsonWebToken getAccessToken() {
        return jwtHolder.updateAndGet(bearerToken -> {
            final JsonWebToken result;
            if (null == bearerToken || bearerToken.isExpired()) {
                result = jwtSupplier.getAccessToken();
            } else {
                result = bearerToken;
            }
            return result;
        });
    }

}
