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
package org.eclipse.ditto.testing.common.connectivity.amqp;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.testing.common.config.TestConfig;

/**
 * Config properties for AMQP connectivity.
 */
@Immutable
public final class AmqpConfig {

    private static final String CONNECTIVITY_PREFIX = "connectivity.";
    private static final String PROPERTY_AMQP10_HOSTNAME = CONNECTIVITY_PREFIX + "amqp10.hostname";
    private static final String PROPERTY_AMQP10_PORT = CONNECTIVITY_PREFIX + "amqp10.port";

    private final String amqp10Hostname;
    private final int amqp10Port;

    private AmqpConfig(final TestConfig testConfig) {
        amqp10Hostname = testConfig.getStringOrThrow(PROPERTY_AMQP10_HOSTNAME);
        amqp10Port = testConfig.getIntOrThrow(PROPERTY_AMQP10_PORT);
    }

    /**
     * Returns an instance of {@code AmqpConfig}.
     *
     * @param testConfig provides the raw testConfig properties.
     * @return the instance.
     * @throws NullPointerException if {@code testConfig} is {@code null}.
     * @throws org.eclipse.ditto.testing.common.config.ConfigError if any config property is either missing or has an
     * inappropriate value.
     */
    public static AmqpConfig of(final TestConfig testConfig) {
        return new AmqpConfig(ConditionChecker.checkNotNull(testConfig, "testConfig"));
    }

    public String getAmqp10Hostname() {
        return amqp10Hostname;
    }

    public int getAmqp10Port() {
        return amqp10Port;
    }

    public URI getAmqp10Uri() {
        return URI.create(String.format("amqp://%s:%d", getAmqp10Hostname(), getAmqp10Port()));
    }

}
