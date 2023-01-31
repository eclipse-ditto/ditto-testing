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
package org.eclipse.ditto.testing.common.client.http;

import java.io.IOException;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.proxy.ProxyServer;
import org.eclipse.ditto.testing.common.CommonTestConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.nio.NioEventLoopGroup;

public class AsyncHttpClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncHttpClientFactory.class);

    private AsyncHttpClientFactory() {
        throw new AssertionError();
    }

    /**
     * Returns a new AsyncHttpClient.
     *
     * @param testConfig the Config for this test run.
     * @return the new AsyncHttpClient instance.
     */
    public static AsyncHttpClient newInstance(final CommonTestConfig testConfig) {
        final ProxyServer proxyServer;
        if (testConfig.isHttpProxyEnabled()) {
            proxyServer = new ProxyServer.Builder(testConfig.getHttpProxyHost(), testConfig.getHttpProxyPort()).build();
        } else {
            proxyServer = null;
        }

        final DefaultAsyncHttpClientConfig.Builder configBuilder = new DefaultAsyncHttpClientConfig.Builder();
        if (proxyServer != null) {
            configBuilder.setProxyServer(proxyServer);
        }

        configBuilder.setConnectTimeout(10000);
        configBuilder.setEventLoopGroup(new NioEventLoopGroup());
        return new DefaultAsyncHttpClient(configBuilder.build());
    }

    /**
     * Closes the AsyncHttpClient.
     * @param client the client to close.
     */
    public static void close(AsyncHttpClient client) {

        if (client != null) {
            try {
                client.close();
            } catch (final IOException e) {
                LOGGER.error("Failed to close client", e);
            }
        }
    }
}
