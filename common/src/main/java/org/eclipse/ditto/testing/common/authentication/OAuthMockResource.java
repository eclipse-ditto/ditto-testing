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

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.jwt.model.JsonWebToken;
import org.eclipse.ditto.testing.common.TestSolutionSupplierRule;
import org.eclipse.ditto.testing.common.junit.ExternalResourcePlus;
import org.junit.runner.Description;

/**
 * This external resource registers a client at the OAuth Mock and provides access tokens for that client.
 * To register the client, the service instance ID of the solution is used, that is provided by the
 * {@link TestSolutionSupplierRule}.
 * <p>
 * The access token is cached to minimize calls to OAuth Mock.
 */
@NotThreadSafe
public final class OAuthMockResource extends ExternalResourcePlus implements AccessTokenSupplier<JsonWebToken> {

    private final OAuthMockConfig oAuthMockConfig;
    private final TestSolutionSupplierRule testSolutionSupplierRule;

    private AccessTokenSupplier<JsonWebToken> accessTokenSupplier;

    private OAuthMockResource(final OAuthMockConfig oAuthMockConfig,
            final TestSolutionSupplierRule testSolutionSupplierRule) {

        this.oAuthMockConfig = oAuthMockConfig;
        this.testSolutionSupplierRule = testSolutionSupplierRule;

        accessTokenSupplier = null;
    }

    public static OAuthMockResource newInstance(final OAuthMockConfig oAuthMockConfig,
            final TestSolutionSupplierRule testSolutionSupplierRule) {

        return new OAuthMockResource(checkNotNull(oAuthMockConfig, "oAuthMockConfig"),
                checkNotNull(testSolutionSupplierRule, "testSolutionSupplierRule"));
    }

    @Override
    protected void before(final Description description) throws Throwable {
        super.before(description);
        accessTokenSupplier = AccessTokenSuppliers.getCachingAuthMockJwtSupplier(oAuthMockConfig,
                testSolutionSupplierRule.getTestSolution());
    }

    @Override
    public JsonWebToken getAccessToken() {
        if (null == accessTokenSupplier) {
            throw new IllegalStateException("The access token gets only available by running the test.");
        } else {
            return accessTokenSupplier.getAccessToken();
        }
    }

}
