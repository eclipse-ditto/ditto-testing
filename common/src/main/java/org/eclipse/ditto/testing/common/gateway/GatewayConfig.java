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
package org.eclipse.ditto.testing.common.gateway;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.testing.common.config.TestConfig;

/**
 * Configuration properties for the Things gateway service.
 */
@Immutable
public final class GatewayConfig {

    private static final String GATEWAY_PREFIX = "gateway.";

    private static final String HTTP_URL_CONFIG_PATH = GATEWAY_PREFIX + "url";
    private static final String WEBSOCKET_URL_CONFIG_PATH = GATEWAY_PREFIX + "ws-url";

    private final URI httpUriApi2;
    private final URI webSocketUriApi2;

    private GatewayConfig(final TestConfig testConfig) {
        final var gatewayHttpUri = testConfig.getUriOrThrow(HTTP_URL_CONFIG_PATH);
        httpUriApi2 = gatewayHttpUri.resolve("/api/2/");
        final var gatewayWebSocketUri = testConfig.getUriOrThrow(WEBSOCKET_URL_CONFIG_PATH);
        webSocketUriApi2 = gatewayWebSocketUri.resolve("/ws/2");
    }

    /**
     * Returns an instance of {@code GatewayConfig}.
     *
     * @param testConfig provides the raw testConfig properties.
     * @return the instance.
     * @throws NullPointerException if {@code testConfig} is {@code null}.
     * @throws org.eclipse.ditto.testing.common.config.ConfigError if any config property is either missing or has an
     * inappropriate value.
     */
    public static GatewayConfig of(final TestConfig testConfig) {
        return new GatewayConfig(ConditionChecker.checkNotNull(testConfig, "testConfig"));
    }

    public URI getHttpUriApi2() {
        return httpUriApi2;
    }

    public URI getWebSocketUriApi2() {
        return webSocketUriApi2;
    }

}
