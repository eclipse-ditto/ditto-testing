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
package org.eclipse.ditto.testing.common.connectivity.mqtt;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.testing.common.config.TestConfig;

/**
 * Config properties for MQTT connectivity.
 */
@Immutable
public final class MqttConfig {

    private static final String CONNECTIVITY_PATH_PREFIX = "connectivity.";
    private static final String MQTT_PATH_PREFIX = CONNECTIVITY_PATH_PREFIX + "mqtt.";
    private static final String PROPERTY_HOSTNAME = MQTT_PATH_PREFIX + "hostname";
    private static final String PROPERTY_PORT_TCP = MQTT_PATH_PREFIX + "port.tcp";
    private static final String PROPERTY_PORT_SSL = MQTT_PATH_PREFIX + "port.ssl";

    private final URI tcpUri;
    private final URI sslUri;

    private MqttConfig(final TestConfig testConfig) {
        final var hostname = testConfig.getStringOrThrow(PROPERTY_HOSTNAME);
        tcpUri = URI.create(String.format("tcp://%s:%d", hostname, testConfig.getIntOrThrow(PROPERTY_PORT_TCP)));
        sslUri = URI.create(String.format("ssl://%s:%d", hostname, testConfig.getIntOrThrow(PROPERTY_PORT_SSL)));
    }

    /**
     * Returns an instance of {@code MqttConfig}.
     *
     * @param testConfig provides the raw testConfig properties.
     * @return the instance.
     * @throws NullPointerException if {@code testConfig} is {@code null}.
     * @throws org.eclipse.ditto.testing.common.config.ConfigError if any config property is either missing or has an
     * inappropriate value.
     */
    public static MqttConfig of(final TestConfig testConfig) {
        return new MqttConfig(ConditionChecker.checkNotNull(testConfig, "testConfig"));
    }

    public URI getTcpUri() {
        return tcpUri;
    }

    public URI getSslUri() {
        return sslUri;
    }

}
