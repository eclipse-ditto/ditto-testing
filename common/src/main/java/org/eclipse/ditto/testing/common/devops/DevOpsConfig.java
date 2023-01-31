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
package org.eclipse.ditto.testing.common.devops;

import java.net.URI;
import java.time.Duration;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.testing.common.config.TestConfig;

/**
 * Configuration properties for DevOps.
 */
@Immutable
public final class DevOpsConfig {

    private static final String DEVOPS_PREFIX = "gateway.devops.";
    private static final String PROPERTY_DEVOPS_URL = DEVOPS_PREFIX + "url";
    private static final String PROPERTY_DEVOPS_COMMAND_TIMEOUT = DEVOPS_PREFIX + "command-timeout";

    private final URI devOpsUri;
    private final Duration devOpsCommandTimeout;

    private DevOpsConfig(final TestConfig testConfig) {
        devOpsUri = testConfig.getUriOrThrow(PROPERTY_DEVOPS_URL);
        devOpsCommandTimeout = testConfig.getDurationOrThrow(PROPERTY_DEVOPS_COMMAND_TIMEOUT);
    }

    /**
     * Returns an instance of {@code DevOpsConfig}.
     *
     * @param testConfig provides the raw testConfig properties.
     * @return the instance.
     * @throws NullPointerException if {@code testConfig} is {@code null}.
     * @throws org.eclipse.ditto.testing.common.config.ConfigError if any config property is either missing or has an
     * inappropriate value.
     */
    public static DevOpsConfig of(final TestConfig testConfig) {
        return new DevOpsConfig(ConditionChecker.checkNotNull(testConfig, "testConfig"));
    }

    public URI getDevOpsUri() {
        return devOpsUri;
    }

    public Duration getDevOpsCommandTimeout() {
        return devOpsCommandTimeout;
    }

}
