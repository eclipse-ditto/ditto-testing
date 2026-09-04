/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assume.assumeTrue;

import javax.annotation.Nullable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;

/**
 * Locks in the test-side persistence switch: {@code persistence.backend} defaults to {@code mongodb} in
 * {@code test-common.conf} and is selected per run with the JVM system property
 * {@code -Dpersistence.backend=postgres} (system properties are the top layer of
 * {@link ConfigFactory#load(com.typesafe.config.Config)}), while the {@code postgres.*} connection keys are part of
 * the plain {@code local} and {@code docker-compose} environments.
 * <p>
 * Uses the protected constructor instead of {@link CommonTestConfig#getInstance()} because the singleton captures the
 * system properties present at class-load time; {@link ConfigFactory#invalidateCaches()} is required because
 * {@code ConfigFactory.systemProperties()} is memoised; system properties are restored after each test.
 * </p>
 */
public final class CommonTestConfigPersistenceBackendTest {

    private static final String PERSISTENCE_BACKEND = "persistence.backend";
    private static final String TEST_ENVIRONMENT = "test.environment";

    private String persistenceBackendBefore;
    private String testEnvironmentBefore;

    /**
     * Starts every test from the {@code local} environment with no backend override, whatever the Maven invocation
     * carried. {@code test.environment} is SET rather than cleared on purpose: surefire's {@code -Dtest=<class>}
     * selector becomes the system property {@code test=<class>}, a scalar that would shadow the whole
     * {@code test.*} object of the conf files; an explicit {@code test.environment} system property makes the
     * property parser drop that scalar again. The previous values are restored in {@link #tearDown()} so that no
     * other test class in the same fork observes this class's overrides (a later
     * {@link CommonTestConfig#getInstance()} would freeze whatever it sees at first use for the whole fork).
     */
    @Before
    public void setUp() {
        persistenceBackendBefore = System.getProperty(PERSISTENCE_BACKEND);
        testEnvironmentBefore = System.getProperty(TEST_ENVIRONMENT);
        System.clearProperty(PERSISTENCE_BACKEND);
        System.setProperty(TEST_ENVIRONMENT, "local");
        ConfigFactory.invalidateCaches();
    }

    @After
    public void tearDown() {
        restore(PERSISTENCE_BACKEND, persistenceBackendBefore);
        restore(TEST_ENVIRONMENT, testEnvironmentBefore);
        ConfigFactory.invalidateCaches();
    }

    private static void restore(final String key, @Nullable final String previousValue) {
        if (previousValue == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, previousValue);
        }
    }

    @Test
    public void persistenceBackendDefaultsToMongoDb() {
        final CommonTestConfig config = new CommonTestConfig();

        assertThat(config.getTestEnvironment()).isEqualTo("local");
        assertThat(config.isLocalOrDockerTestEnvironment()).isTrue();
        assertThat(config.getPersistenceBackend()).isEqualTo("mongodb");
    }

    @Test
    public void systemPropertyOverridesPersistenceBackendFromConf() {
        System.setProperty(PERSISTENCE_BACKEND, "postgres");
        ConfigFactory.invalidateCaches();

        final CommonTestConfig config = new CommonTestConfig();

        assertThat(config.getPersistenceBackend()).isEqualTo("postgres");
    }

    @Test
    public void postgresKeysAreDefinedInLocalEnvironment() {
        assumeTrue("POSTGRES_JDBC_URI hook is set in this shell", System.getenv("POSTGRES_JDBC_URI") == null);

        final CommonTestConfig config = new CommonTestConfig();

        assertThat(config.getPostgresJdbcUri()).isEqualTo("jdbc:postgresql://localhost:5432/ditto");
        assertThat(config.getPostgresUser()).isEqualTo("ditto");
        assertThat(config.getPostgresPassword()).isEqualTo("ditto");
    }

    @Test
    public void postgresKeysAreDefinedInDockerComposeEnvironment() {
        assumeTrue("POSTGRES_JDBC_URI hook is set in this shell", System.getenv("POSTGRES_JDBC_URI") == null);
        System.setProperty(TEST_ENVIRONMENT, "docker-compose");
        ConfigFactory.invalidateCaches();

        final CommonTestConfig config = new CommonTestConfig();

        assertThat(config.getTestEnvironment()).isEqualTo("docker-compose");
        assertThat(config.isLocalOrDockerTestEnvironment()).isTrue();
        assertThat(config.getPostgresJdbcUri()).isEqualTo("jdbc:postgresql://postgres:5432/ditto");
        assertThat(config.getPostgresUser()).isEqualTo("ditto");
        assertThat(config.getPostgresPassword()).isEqualTo("ditto");
    }

    /**
     * The suffixed environments {@code docker-compose-postgres} / {@code local-postgres} were removed in favour of
     * {@code -Dpersistence.backend=postgres}; a stale command line must fail with the missing file's name instead of
     * silently loading only {@code test-common.conf} (which would skip every environment-gated suite).
     */
    @Test
    public void removedSuffixedEnvironmentNameFailsFast() {
        System.setProperty(TEST_ENVIRONMENT, "docker-compose-postgres");
        ConfigFactory.invalidateCaches();

        assertThatThrownBy(CommonTestConfig::new)
                .isInstanceOf(ConfigException.IO.class)
                .hasMessageContaining("test-common-docker-compose-postgres");
    }

}
