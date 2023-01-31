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

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import org.asynchttpclient.AsyncHttpClient;
import org.eclipse.ditto.testing.common.client.http.AsyncHttpClientFactory;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;

/**
 * Bundles all solution stuff together. This includes the {@link Solution}
 * itself and also a {@link org.eclipse.ditto.testing.common.client.oauth.AuthClient} to call Ditto APIs in
 * the context of the solution.
 */
public final class TestingContext {

    private static final String DEFAULT_SCOPES = "system-test";

    private final Solution solution;
    private final AuthClient oAuthClient;

    private TestingContext(final Solution solution, final AuthClient oAuthClient) {
        this.solution = solution;
        this.oAuthClient = oAuthClient;
    }

    public Solution getSolution() {
        return solution;
    }

    public AuthClient getOAuthClient() {
        return oAuthClient;
    }

    public static TestingContext newInstance(final Solution solution, final AuthClient oAuthClient) {
        checkNotNull(solution, "solution");
        checkNotNull(oAuthClient, "oAuthClient");
        return new TestingContext(solution, oAuthClient);
    }

    public static TestingContext withGeneratedMockClient(final Solution solution, final CommonTestConfig config) {
        checkNotNull(config, "config");

        final AsyncHttpClient httpClient = AsyncHttpClientFactory.newInstance(config);
        final AuthClient oAuthClient = AuthClient.newInstance(config.getOAuthMockTokenEndpoint(),
                ThingsSubjectIssuer.DITTO,
                solution.getUsername(),
                solution.getSecret(),
                DEFAULT_SCOPES,
                httpClient);

        return newInstance(solution, oAuthClient);
    }

}
