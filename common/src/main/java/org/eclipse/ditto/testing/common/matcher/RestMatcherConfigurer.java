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
package org.eclipse.ditto.testing.common.matcher;

import java.util.UUID;

import javax.annotation.Nullable;

import org.eclipse.ditto.testing.common.HttpHeader;

/**
 * Configures Rest Matchers.
 */
public final class RestMatcherConfigurer {

    @Nullable private final String jwt;
    @Nullable private final PreAuthenticatedAuth preAuthenticatedAuth;

    private RestMatcherConfigurer(@Nullable final String jwt,
            @Nullable final PreAuthenticatedAuth preAuthenticatedAuth) {
        this.jwt = jwt;
        this.preAuthenticatedAuth = preAuthenticatedAuth;
    }

    public static RestMatcherConfigurer empty() {
        return new RestMatcherConfigurer(null, null);
    }

    public static RestMatcherConfigurer withJwt(final String jwt) {
        return new RestMatcherConfigurer(jwt, null);
    }

    public <T extends HttpVerbMatcher> T configure(final T matcher) {
        matcher.withHeader(HttpHeader.X_CORRELATION_ID, String.valueOf(UUID.randomUUID()));

        if (null != jwt) {
            matcher.withJWT(jwt);
        }

        if (null != preAuthenticatedAuth) {
            matcher.withPreAuthenticatedAuth(preAuthenticatedAuth);
        }

        return matcher;
    }

}
