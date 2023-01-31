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
package org.eclipse.ditto.testing.common.connectivity.http;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.testing.common.config.TestConfig;

/**
 * Config properties for HTTP Push connectivity.
 */
@Immutable
public final class HttpPushConfig {

    private static final String CONNECTIVITY_PATH_PREFIX = "connectivity.";
    private static final String MQTT_PATH_PREFIX = CONNECTIVITY_PATH_PREFIX + "http.";
    private static final String PROPERTY_HOSTNAME = MQTT_PATH_PREFIX + "hostname";

    private final String hostName;

    private HttpPushConfig(final TestConfig testConfig) {
        this.hostName = testConfig.getStringOrThrow(PROPERTY_HOSTNAME);
    }

    /**
     * Returns an instance of {@code HttpPushConfig}.
     *
     * @param testConfig provides the raw testConfig properties.
     * @return the instance.
     * @throws NullPointerException if {@code testConfig} is {@code null}.
     * @throws org.eclipse.ditto.testing.common.config.ConfigError if any config property is either missing or has an
     * inappropriate value.
     */
    public static HttpPushConfig of(final TestConfig testConfig) {
        return new HttpPushConfig(ConditionChecker.checkNotNull(testConfig, "testConfig"));
    }

    public String getHostName() {
        return hostName;
    }

}
