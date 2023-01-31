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
package org.eclipse.ditto.testing.common.config;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.time.Duration;

import javax.annotation.Nullable;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;

/**
 * A thin wrapper around a {@link Config}.
 * <p>
 * The main purpose of this class is to load and merge various config files based on the determined
 * {@link TestEnvironment} to obtain and wrap one big {@link Config} containing all relevant config properties for
 * testing.
 * </p>
 * <p>
 * Getters for config properties throw a {@link ConfigError} if the property does either not exist or if the value is
 * inappropriate.
 * </p>
 * <p>
 * Furthermore this class provides the determined {@code TestEnvironment} without having to determine it over and over.
 * </p>
 */
public final class TestConfig {

    private static final String BASE_CONFIG_RESOURCE_NAME = "test-common.conf";
    private static final String TEST_ENVIRONMENT_CONFIG_PATH = "test.environment";
    private static final String MODULE_CONFIG_RESOURCE_NAME = "test.conf";

    private static final Logger LOGGER = LoggerFactory.getLogger(TestConfig.class);

    @Nullable private static TestConfig instance = null;

    private final Config config;
    private final TestEnvironment testEnvironment;

    private TestConfig(final Config config, final TestEnvironment testEnvironment) {
        this.config = config;
        this.testEnvironment = testEnvironment;
    }

    /**
     * Returns the <em>singleton instance</em> of {@code TestConfig}.
     *
     * @return the singleton {@code TestConfig}.
     */
    public static TestConfig getInstance() {
        var result = instance;
        if (null == result) {
            final var baseConfig = getBaseConfig();
            final var testEnvironment = determineTestEnvironment(baseConfig);
            result = new TestConfig(tryToLoadTestConfig(baseConfig, testEnvironment), testEnvironment);
            instance = result;
        }
        return result;
    }

    private static Config getBaseConfig() {
        final var result = ConfigFactory.parseResourcesAnySyntax(BASE_CONFIG_RESOURCE_NAME);
        LOGGER.debug("Parsed base config: {}", result);
        return result;
    }

    private static TestEnvironment determineTestEnvironment(final Config baseConfig) {

        // Have to load the baseConfig otherwise system property won't be resolved.
        final var loadedBaseConfig = ConfigFactory.load(baseConfig);
        final var configPath = TEST_ENVIRONMENT_CONFIG_PATH;
        if (!loadedBaseConfig.hasPath(configPath)) {
            final var pattern = "Test environment must be defined with property <{0}>.";
            throw new ConfigError(MessageFormat.format(pattern, configPath));
        }
        final var testEnvironmentConfigValue = loadedBaseConfig.getString(configPath);
        final var result = TestEnvironment.getForString(testEnvironmentConfigValue)
                .orElseThrow(() -> {
                    final var pattern = "Failed to determine test environment: <{0}> is unknown.";
                    throw new ConfigError(MessageFormat.format(pattern, testEnvironmentConfigValue));
                });
        LOGGER.info("Running against test environment <{}>.", result);
        return result;
    }

    private static Config tryToLoadTestConfig(final Config baseConfig, final TestEnvironment testEnvironment) {
        try {
            return loadTestConfig(baseConfig, testEnvironment);
        } catch (final ConfigException e) {
            throw new ConfigError(MessageFormat.format("Failed to get test config: {0}", e.getMessage()), e);
        }
    }

    private static Config loadTestConfig(final Config baseConfig, final TestEnvironment testEnvironment) {
        final var environmentSpecificCommonConfig = getEnvironmentSpecificCommonConfig(testEnvironment);

        final var mergedBaseConfig = environmentSpecificCommonConfig.withFallback(baseConfig);
        LOGGER.debug("Merged base config <{}>.", mergedBaseConfig);

        final var environmentSpecificModuleConfig = getEnvironmentSpecificModuleConfig(testEnvironment);
        final var mergedModuleConfig = environmentSpecificModuleConfig.withFallback(getModuleConfig());
        LOGGER.debug("Merged module config <{}>.", mergedModuleConfig);

        final var finalConfig = mergedModuleConfig.withFallback(mergedBaseConfig);
        LOGGER.debug("Final config <{}>.", finalConfig);

        final var result = ConfigFactory.load(finalConfig);
        LOGGER.debug("Loaded final test config.");
        return result;
    }

    private static Config getEnvironmentSpecificCommonConfig(final TestEnvironment testEnvironment) {
        final var resourceBasename = String.format("test-common-%s.conf", testEnvironment);
        final var configParseOptions = ConfigParseOptions.defaults().setAllowMissing(false);
        final var result = ConfigFactory.parseResourcesAnySyntax(resourceBasename, configParseOptions);
        LOGGER.debug("Using environment specific common base config <{}>.", result);
        return result;
    }

    private static Config getEnvironmentSpecificModuleConfig(final TestEnvironment testEnvironment) {

        // module/env-specific config is also optional
        final var resourceBasename = String.format("test-%s.conf", testEnvironment);
        final var result = ConfigFactory.parseResourcesAnySyntax(resourceBasename,
                ConfigParseOptions.defaults().setAllowMissing(true));
        LOGGER.debug("Using environment specific module config <{}>.", result);
        return result;
    }

    private static Config getModuleConfig() {

        // Module specific config is optional: they can contain an optional test.conf overwriting test-common.conf.
        final var result = ConfigFactory.parseResourcesAnySyntax(MODULE_CONFIG_RESOURCE_NAME,
                ConfigParseOptions.defaults().setAllowMissing(true));
        LOGGER.debug("Using module config <{}>.", result);
        return result;
    }

    public TestEnvironment getTestEnvironment() {
        return testEnvironment;
    }

    public URI getUriOrThrow(final String configPath) {
        final var configString = getStringOrThrow(configPath);
        try {
            return new URI(configString);
        } catch (final URISyntaxException e) {
            final var pattern = "Failed to get URI for value <{0}> at path <{1}>: {2}";
            throw new ConfigError(MessageFormat.format(pattern, configString, configPath, e.getMessage()), e);
        }
    }

    public String getStringOrThrow(final String configPath) {
        try {
            return config.getString(requireConfigPathNotNull(configPath));
        } catch (final ConfigException e) {
            throw new ConfigError(MessageFormat.format("Failed to get String at <{0}>.", configPath), e);
        }
    }

    private static String requireConfigPathNotNull(final String configPath) {
        return ConditionChecker.checkNotNull(configPath, "configPath");
    }

    public Duration getDurationOrThrow(final String configPath) {
        try {
            return config.getDuration(requireConfigPathNotNull(configPath));
        } catch (final ConfigException e) {
            throw new ConfigError(MessageFormat.format("Failed to get Duration at <{0}>.", configPath), e);
        }
    }

    public int getIntOrThrow(final String configPath) {
        try {
            return config.getInt(configPath);
        } catch (final ConfigException e) {
            throw new ConfigError(MessageFormat.format("Failed to get int at <{0}>.", configPath), e);
        }
    }

    public boolean getBooleanOrThrow(final String configPath) {
        try {
            return config.getBoolean(configPath);
        } catch (final ConfigException e) {
            throw new ConfigError(MessageFormat.format("Failed to get boolean at <{0}>.", configPath), e);
        }
    }

    public Config getConfigOrThrow(final String configPath) {
        try {
            return config.getConfig(configPath);
        } catch (final ConfigException e) {
            throw new ConfigError(MessageFormat.format("Failed to get config at <{0}>.", configPath), e);
        }
    }

}
