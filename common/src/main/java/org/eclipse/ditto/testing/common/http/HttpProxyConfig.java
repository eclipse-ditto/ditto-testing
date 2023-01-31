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
package org.eclipse.ditto.testing.common.http;

import java.text.MessageFormat;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.testing.common.config.ConfigError;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration properties for a HTTP proxy.
 */
@Immutable
public final class HttpProxyConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpProxyConfig.class);

    private static final String PROPERTY_SETUP_HTTP_PROXY_ENABLED = "setup.http.proxy.enabled";
    private static final String SYSTEM_PROPERTY_PROXY_HOST = "http.proxyHost";
    private static final String SYSTEM_PROPERTY_PROXY_PORT = "http.proxyPort";

    private static final String DEFAULT_PROXY_HOST = "localhost";
    private static final int DEFAULT_PROXY_PORT = 3128;

    private final boolean proxyEnabled;
    private final String proxyHost;
    private final int proxyPort;

    private HttpProxyConfig(final TestConfig testConfig) {
        proxyEnabled = testConfig.getBooleanOrThrow(PROPERTY_SETUP_HTTP_PROXY_ENABLED);
        proxyHost = determineProxyHost();
        proxyPort = determineProxyPort();
    }

    private static String determineProxyHost() {
        @Nullable var result = System.getProperty(SYSTEM_PROPERTY_PROXY_HOST);
        if (null == result) {
            LOGGER.info("System property <{}> is not set. Falling back to default <{}>.",
                    SYSTEM_PROPERTY_PROXY_HOST,
                    DEFAULT_PROXY_HOST);
            result = DEFAULT_PROXY_HOST;
        }
        return result;
    }

    private static int determineProxyPort() {
        final int result;
        @Nullable final var proxyPortSystemProperty = System.getProperty(SYSTEM_PROPERTY_PROXY_PORT);
        if (null != proxyPortSystemProperty) {
            try {
                result = Integer.parseInt(proxyPortSystemProperty);
            } catch (final NumberFormatException e) {
                final var pattern = "Failed to parse value <{0}> as int for system property <{1}>: {2}";
                throw new ConfigError(MessageFormat.format(pattern,
                        proxyPortSystemProperty,
                        SYSTEM_PROPERTY_PROXY_HOST,
                        e.getMessage()), e);
            }
        } else {
            LOGGER.info("System property <{}> is not set. Falling back to default <{}>.",
                    SYSTEM_PROPERTY_PROXY_PORT,
                    DEFAULT_PROXY_PORT);
            result = DEFAULT_PROXY_PORT;
        }
        return result;
    }

    public static HttpProxyConfig of(final TestConfig testConfig) {
        return new HttpProxyConfig(ConditionChecker.checkNotNull(testConfig, "testConfig"));
    }

    public boolean isHttpProxyEnabled() {
        return proxyEnabled;
    }

    public String getHttpProxyHost() {
        return proxyHost;
    }

    public int getHttpProxyPort() {
        return proxyPort;
    }

    @Override
    public boolean equals(@Nullable final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final var that = (HttpProxyConfig) o;
        return proxyEnabled == that.proxyEnabled && proxyPort == that.proxyPort &&
                Objects.equals(proxyHost, that.proxyHost);
    }

    @Override
    public int hashCode() {
        return Objects.hash(proxyEnabled, proxyHost, proxyPort);
    }

}
