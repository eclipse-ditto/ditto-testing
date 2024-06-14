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

import static java.util.Objects.requireNonNull;
import static org.eclipse.ditto.testing.common.IntegrationTest.TEST_CONFIG;

import java.util.UUID;
import java.util.function.Supplier;

import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.testing.common.client.http.AsyncHttpClientFactory;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.common.matcher.PostMatcher;
import org.eclipse.ditto.testing.common.util.LazySupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class sets up a Ditto environment for testing.
 */
public final class ServiceEnvironment {

    public static final String DEFAULT_NAMESPACE = "namespace.default";
    public static final String NAMESPACE_TWO = "namespace.two";
    public static final String NAMESPACE_THREE = "namespace.three";
    public static final String NAMESPACE_FOUR = "namespace.four";
    public static final String NAMESPACE_FIVE = "namespace.five";

    private static final Solution TESTING_SOLUTIONS[] = {
            new Solution("user", "password", DEFAULT_NAMESPACE),
            new Solution("user2", "password2", NAMESPACE_TWO),
            new Solution("user3", "password3", NAMESPACE_THREE),
            new Solution("user4", "password4", NAMESPACE_FOUR),
            new Solution("user5", "password5", NAMESPACE_FIVE)
    };
    private static final TestingContext TESTING_CONTEXT = TestingContext.withGeneratedMockClient(
            new Solution("user", "password", DEFAULT_NAMESPACE), TEST_CONFIG);

    /**
     * Prefix for the Default namespace of the default Solution.
     */
    private static final String DEFAULT_NAMESPACE_PREFIX = DEFAULT_NAMESPACE + ".";

    private final Supplier<TestingContext> defaultTestingContext;
    private final Supplier<TestingContext> testingContext2;
    private final Supplier<TestingContext> testingContext3;
    private final Supplier<TestingContext> testingContext4;
    private final Supplier<TestingContext> testingContext5;

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceEnvironment.class);

    private ServiceEnvironment(final Supplier<TestingContext> defaultTestingContext,
            final Supplier<TestingContext> testingContext2,
            final Supplier<TestingContext> testingContext3,
            final Supplier<TestingContext> testingContext4,
            final Supplier<TestingContext> testingContext5) {

        this.defaultTestingContext = requireNonNull(defaultTestingContext);
        this.testingContext2 = requireNonNull(testingContext2);
        this.testingContext3 = requireNonNull(testingContext3);
        this.testingContext4 = requireNonNull(testingContext4);
        this.testingContext5 = requireNonNull(testingContext5);
    }

    /**
     * Creates a new instance from the given {@code testConfig}.
     *
     * @param testConfig the test configuration
     * @return the instance
     */
    public static ServiceEnvironment from(final CommonTestConfig testConfig) {
        requireNonNull(testConfig);
        LOGGER.info("Instantiate ServiceEnvironment");

        final Supplier<TestingContext> defaultTestingContext = getTestingContextSupplier(testConfig, 1);
        final Supplier<TestingContext> testingContext2 = getTestingContextSupplier(testConfig, 2);
        final Supplier<TestingContext> testingContext3 = getTestingContextSupplier(testConfig, 3);
        final Supplier<TestingContext> testingContext4 = getTestingContextSupplier(testConfig, 4);

        final Supplier<TestingContext> testingContext5 = getTestingContextSupplier(testConfig, 3);

        return new ServiceEnvironment(defaultTestingContext, testingContext2, testingContext3, testingContext4, testingContext5);
    }

    public String getDefaultNamespaceName() {
        return DEFAULT_NAMESPACE;
    }

    public String getSecondaryNamespaceName() {
        return DEFAULT_NAMESPACE + ".secondary";
    }

    /**
     * Creates a new instance from the given {@code testConfig} for performance tests. In this case, the default
     * TestingContext should be reused accross JVM restarts: i.e. it must have a default namespace with fixed (i.e.
     * non-random) name (needed for perf-tests) and a static secret.
     *
     * @param testConfig the test configuration
     * @return the instance
     */
    public static ServiceEnvironment forPerformanceTests(final CommonTestConfig testConfig) {
        requireNonNull(testConfig);
        return forPerformanceTests(testConfig);
    }

    public Solution createSolution(final CharSequence namespace) {

        final String randomUsername = "user-" + UUID.randomUUID();
        return new Solution(randomUsername, "password", namespace.toString());
    }

    public static Solution createSolutionWithRandomUsernameRandomNamespace() {

        final String randomNamespace = "org.eclipse.ditto.ns.random." + randomString();
        final String randomUsername = "user-" + UUID.randomUUID();
        return new Solution(randomUsername, "password", randomNamespace);
    }

    public static String randomString() {
        return RandomStringUtils.randomAlphabetic(1) + RandomStringUtils.randomAlphanumeric(10);
    }

    public TestingContext getDefaultTestingContext() {
        return this.defaultTestingContext.get();
    }

    public TestingContext getTestingContext2() {
        return this.testingContext2.get();
    }

    public String getTesting2NamespaceName() {
        return NAMESPACE_TWO;
    }

    public TestingContext getTestingContext3() {
        return this.testingContext3.get();
    }

    public TestingContext getTestingContext4() {
        return this.testingContext4.get();
    }

    public TestingContext getTestingContext5() {
        return this.testingContext5.get();
    }

    public String getDefaultAuthUsername() {
        return getDefaultTestingContext().getSolution().getUsername();
    }


    public static String createRandomDefaultNamespace() {
        return createRandomNamespace(DEFAULT_NAMESPACE_PREFIX);
    }

    private static String createRandomNamespace(final String namespacePrefix) {
        requireNonNull(namespacePrefix);
        return namespacePrefix + randomString();
    }

    public PostMatcher postPiggy(final String piggyBackSubUrl, final JsonObject jsonPiggyCommand,
            final String targetActorSelection) {

        return PiggyBackCommander.getInstance(piggyBackSubUrl).preparePost(jsonPiggyCommand, targetActorSelection)
                .withExpectedStatus(HttpStatus.OK).build();

    }

    private static Supplier<TestingContext> getTestingContextSupplier(
            final CommonTestConfig config, final int clientNumber) {
        return LazySupplier.fromLoader(() -> {
            final TestingContext testingContext;
            if (config.isLocalOrDockerTestEnvironment() || config.getBasicAuth().isEnabled()) {
                testingContext = TestingContext.withGeneratedMockClient(TESTING_SOLUTIONS[clientNumber], config);
            } else {
                final AuthClient client = AuthClient.newInstance(config.getOAuthTokenEndpoint(),
                        config.getOAuthIssuer(),
                        config.getOAuthClientId(clientNumber),
                        config.getOAuthClientSecret(clientNumber),
                        config.getOAuthClientScope(clientNumber),
                        AsyncHttpClientFactory.newInstance(config),
                        config.getOAuthClientScope(clientNumber),
                        '@' + config.getOAuthClientId(clientNumber));
                testingContext = TestingContext.newInstance(TESTING_SOLUTIONS[clientNumber], client, config.getBasicAuth());
            }
            return testingContext;
        });
    }

}
