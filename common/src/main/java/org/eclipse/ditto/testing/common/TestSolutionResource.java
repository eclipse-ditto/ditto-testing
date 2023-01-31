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

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.jwt.model.JsonWebToken;
import org.eclipse.ditto.testing.common.authentication.AccessTokenSupplier;
import org.eclipse.ditto.testing.common.authentication.AccessTokenSuppliers;
import org.eclipse.ditto.testing.common.authentication.OAuthMockConfig;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.config.TestEnvironment;
import org.junit.rules.ExternalResource;

/**
 * Provides a solution for system tests and acts as {@link AccessTokenSupplier} in the context of the provided solution.
 * <p>
 * The test environment provided by {@link TestConfig#getTestEnvironment()} determines the provided solution:
 * </p>
 * <p>
 * If test environment is {@link TestEnvironment#LOCAL} or {@link TestEnvironment#DOCKER_COMPOSE} then a solution gets
 * created via piggyback command before test and deleted via piggyback command after test.
 * The properties of the solution to be created are hard-coded in this class.
 * </p>
 * <p>
 * Every other test environment is a cloud system where test solutions already exist.
 * The properties of the solution to be used are defined by test config path {@code solutions.one}.
 * Before the test, the old namespaces of that solution are deleted and new ones get created instead.
 * </p>
 */
@NotThreadSafe
public final class TestSolutionResource extends ExternalResource
        implements TestSolutionSupplierRule, AccessTokenSupplier<JsonWebToken> {

    private final TestConfig testConfig;
    private Solution solution;
    private AccessTokenSupplier<JsonWebToken> accessTokenSupplier;

    private TestSolutionResource(final TestConfig testConfig) {
        this.testConfig = testConfig;
        solution = null;
        accessTokenSupplier = null;
    }

    /**
     * Returns a new instance of {@code TestSolutionResource}.
     *
     * @param testConfig the test configuration properties.
     * @return the instance.
     * @throws NullPointerException if {@code testConfig} is {@code null}.
     */
    public static TestSolutionResource newInstance(final TestConfig testConfig) {
        ConditionChecker.checkNotNull(testConfig, "testConfig");
        return new TestSolutionResource(
                testConfig
        );
    }

    @Override
    protected void before() throws Throwable {
        solution = createDefaultSolution();
        accessTokenSupplier = getCachingAuthMockJwtSupplier();
    }

    private AccessTokenSupplier<JsonWebToken> getCachingAuthMockJwtSupplier() {
        return AccessTokenSuppliers.getCachingAuthMockJwtSupplier(OAuthMockConfig.of(testConfig), solution);
    }

    private Solution createDefaultSolution() {
        return ServiceEnvironment.createSolutionWithRandomUsernameRandomNamespace();
    }

    /**
     * Returns the test solution.
     *
     * @return the test solution.
     * @throws IllegalStateException if this method was called before a test was run.
     */
    @Override
    public Solution getTestSolution() {
        if (null == solution) {
            throw new IllegalStateException("The test solution becomes only available after running the test.");
        } else {
            return solution;
        }
    }

    @Override
    public JsonWebToken getAccessToken() {
        if (null == accessTokenSupplier) {
            throw new IllegalStateException("The JsonWebToken becomes only available after running the test.");
        } else {
            return accessTokenSupplier.getAccessToken();
        }
    }

}

