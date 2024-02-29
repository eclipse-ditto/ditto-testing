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
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.client.BasicAuth;

/**
 * Configures Rest Matchers.
 */
public final class RestMatcherConfigurer {

    @Nullable private final String jwt;
    @Nullable private final PreAuthenticatedAuth preAuthenticatedAuth;
    @Nullable private final BasicAuth basicAuth;

    private RestMatcherConfigurer(@Nullable final String jwt,
            @Nullable final PreAuthenticatedAuth preAuthenticatedAuth,
            @Nullable final BasicAuth basicAuth) {
        this.jwt = jwt;
        this.preAuthenticatedAuth = preAuthenticatedAuth;
        this.basicAuth = basicAuth;
    }

    public static RestMatcherConfigurer empty() {
        return new RestMatcherConfigurer(null, null, null);
    }

    public static RestMatcherConfigurer withJwt(final String jwt) {
        return new RestMatcherConfigurer(jwt, null, null);
    }

    public static RestMatcherConfigurer withBasicAuth(final BasicAuth basicAuth) {
        return new RestMatcherConfigurer(null, null, basicAuth);
    }

    public static RestMatcherConfigurer withConfiguredAuth(TestingContext context) {
        final BasicAuth ba = context.getBasicAuth();
        if (null != ba && ba.isEnabled()) {
            return new RestMatcherConfigurer(null, null, BasicAuth.newInstance(true, ba.getUsername(), ba.getPassword()));
        } else {
            return new RestMatcherConfigurer(context.getOAuthClient().getAccessToken(), null, null);
        }
    }

    public <T extends HttpVerbMatcher> T configure(final T matcher) {
        matcher.withHeader(HttpHeader.X_CORRELATION_ID, String.valueOf(UUID.randomUUID()));

        if (null != basicAuth && basicAuth.isEnabled()) {
            matcher.withBasicAuth(basicAuth.getUsername(), basicAuth.getPassword());
        } else {
            if (null != jwt) {
                matcher.withJWT(jwt);
            }

            if (null != preAuthenticatedAuth) {
                matcher.withPreAuthenticatedAuth(preAuthenticatedAuth);
            }
        }

        return matcher;
    }

}
