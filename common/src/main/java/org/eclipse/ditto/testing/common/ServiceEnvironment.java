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

import org.apache.commons.lang3.RandomStringUtils;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.connectivity.model.signals.commands.ConnectivityCommand;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.testing.common.client.ConnectionsClient;
import org.eclipse.ditto.testing.common.matcher.PostMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.restassured.response.Response;

/**
 * This class sets up a Ditto environment for testing.
 */
public final class ServiceEnvironment {

    public static final String DEFAULT_NAMESPACE = "namespace.default";
    public static final TestingContext TESTING_CONTEXT = TestingContext.withGeneratedMockClient(
            new Solution("user", "password", DEFAULT_NAMESPACE), TEST_CONFIG);

    /**
     * Prefix for the Default namespace of the default solution.
     */
    private static final String DEFAULT_NAMESPACE_PREFIX = DEFAULT_NAMESPACE + ".";

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceEnvironment.class);
    public static final String NAMESPACE_TWO = "namespace.two";
    public static final TestingContext TESTING_CONTEXT2 = TestingContext.withGeneratedMockClient(
            new Solution("user2", "password2", NAMESPACE_TWO), TEST_CONFIG);
    public static final String NAMESPACE_THREE = "namespace.three";
    public static final TestingContext TESTING_CONTEXT3 = TestingContext.withGeneratedMockClient(
            new Solution("user3", "password3", NAMESPACE_THREE), TEST_CONFIG);
    public static final String NAMESPACE_FOUR = "namespace.four";
    public static final TestingContext TESTING_CONTEXT4 = TestingContext.withGeneratedMockClient(
            new Solution("user4", "password4", NAMESPACE_FOUR), TEST_CONFIG);
    public static final String NAMESPACE_FIVE = "namespace.five";
    public static final TestingContext TESTING_CONTEXT5 = TestingContext.withGeneratedMockClient(
            new Solution("user5", "password5", NAMESPACE_FIVE), TEST_CONFIG);


    private final CommonTestConfig testConfig;

    private ServiceEnvironment(final CommonTestConfig testConfig) {

        this.testConfig = requireNonNull(testConfig);
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
        return new ServiceEnvironment(testConfig);
    }

    public String getDefaultNamespaceName() {
        return DEFAULT_NAMESPACE;
    }

    public String getSecondaryNamespaceName() {
        return DEFAULT_NAMESPACE + ".secondary";
    }

    /**
     * Creates a new instance from the given {@code testConfig} for performance tests. In this case, the default
     * solution should be reused accross JVM restarts: i.e. it must have a default namespace with fixed (i.e.
     * non-random) name (needed for perf-tests) and a static secret.
     *
     * @param testConfig the test configuration
     * @return the instance
     */
    public static ServiceEnvironment forPerformanceTests(final CommonTestConfig testConfig) {
        requireNonNull(testConfig);
        return forPerformanceTests(testConfig);
    }

    public TestingContext getDefaultTestingContext() {
        return TESTING_CONTEXT;
    }

    public TestingContext getTestingContext2() {
        return TESTING_CONTEXT2;
    }

    public String getTesting2NamespaceName() {
        return NAMESPACE_TWO;
    }

    public TestingContext getTestingContext3() {
        return TESTING_CONTEXT3;
    }

    public TestingContext getTestingContext4() {
        return TESTING_CONTEXT4;
    }

    public TestingContext getTestingContext5() {
        return TESTING_CONTEXT5;
    }

    public String getDefaultAuthUsername() {
        return getDefaultTestingContext().getSolution().getUsername();
    }


    public static String createRandomDefaultNamespace() {
        final String randomNamespace = createRandomNamespace(DEFAULT_NAMESPACE_PREFIX);
        return createDefaultNamespace(randomNamespace);
    }

    private static String createRandomNamespace(final String namespacePrefix) {
        requireNonNull(namespacePrefix);
        return namespacePrefix + RandomStringUtils.randomAlphabetic(1) + RandomStringUtils.randomAlphanumeric(10);
    }


    private static String createDefaultNamespace(final CharSequence name) {
        return DEFAULT_NAMESPACE_PREFIX + name;
    }
    public Response executeConnectionsCommand(final ConnectivityCommand<?> command,
            final HttpStatus... allowedPiggyCommandResponseStatuses) {

        final ConnectionsClient connectionsClient = ConnectionsClient.getInstance();
        return connectionsClient.executeCommand(command, allowedPiggyCommandResponseStatuses);
    }

    public PostMatcher postPiggy(final String piggyBackSubUrl, final JsonObject jsonPiggyCommand,
            final String targetActorSelection) {

        return PiggyBackCommander.getInstance(piggyBackSubUrl).preparePost(jsonPiggyCommand, targetActorSelection)
                .withExpectedStatus(HttpStatus.OK).build();

    }

    public String getUnauthorizedAccessToken() {
        return "";
    }
}
