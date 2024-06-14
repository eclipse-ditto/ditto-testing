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
package org.eclipse.ditto.testing.common.ws;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.asynchttpclient.proxy.ProxyServer;
import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.jwt.model.JsonWebToken;
import org.eclipse.ditto.testing.common.authentication.AccessTokenSupplier;
import org.eclipse.ditto.testing.common.client.BasicAuth;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.gateway.GatewayConfig;
import org.eclipse.ditto.testing.common.http.HttpProxyConfig;

/**
 * Factory for creating instances of {@link ThingsWebsocketClient}.
 */
@Immutable
public final class ThingsWebSocketClientFactory {

    private ThingsWebSocketClientFactory() {
        throw new AssertionError();
    }

    public static ThingsWebsocketClient getWebSocketClient(final TestConfig testConfig,
            final AccessTokenSupplier<JsonWebToken> jwtSupplier,
            final BasicAuth basicAuth,
            final DittoHeaders additionalHeaders) {

        ConditionChecker.checkNotNull(testConfig, "testConfig");
        ConditionChecker.checkNotNull(jwtSupplier, "jwtSupplier")   ;
        ConditionChecker.checkNotNull(additionalHeaders, "additionalHeaders")   ;

        final var jsonWebToken = jwtSupplier.getAccessToken();
        return ThingsWebsocketClient.newInstance(getEndpoint(testConfig),
                jsonWebToken.getToken(),
                basicAuth,
                additionalHeaders,
                getProxyServerOrNull(testConfig),
                ThingsWebsocketClient.AuthMethod.HEADER);
    }

    private static String getEndpoint(final TestConfig testConfig) {
        final var gatewayConfig = GatewayConfig.of(testConfig);
        final var webSocketUriApi2 = gatewayConfig.getWebSocketUriApi2();
        return webSocketUriApi2.toString();
    }

    @Nullable
    private static ProxyServer getProxyServerOrNull(final TestConfig testConfig) {
        final ProxyServer result;
        final var httpProxyConfig = HttpProxyConfig.of(testConfig);
        if (httpProxyConfig.isHttpProxyEnabled()) {
            result = new ProxyServer.Builder(httpProxyConfig.getHttpProxyHost(),
                    httpProxyConfig.getHttpProxyPort()).build();
        } else {
            result = null;
        }
        return result;
    }

}
