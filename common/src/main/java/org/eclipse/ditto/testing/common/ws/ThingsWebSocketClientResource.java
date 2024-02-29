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

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.text.MessageFormat;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.jwt.model.JsonWebToken;
import org.eclipse.ditto.testing.common.authentication.AccessTokenSupplier;
import org.eclipse.ditto.testing.common.client.BasicAuth;
import org.eclipse.ditto.testing.common.config.TestConfig;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This external resource creates and provides a {@link ThingsWebsocketClient}.
 * The client gets connected before the test and closed afterwards.
 */
@NotThreadSafe
public final class ThingsWebSocketClientResource extends ExternalResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThingsWebSocketClientResource.class);

    private final TestConfig testConfig;
    private final AccessTokenSupplier<JsonWebToken> jwtSupplier;

    @Nullable private ThingsWebsocketClient thingsWebsocketClient;

    private ThingsWebSocketClientResource(final TestConfig testConfig,
            final AccessTokenSupplier<JsonWebToken> jwtSupplier) {

        this.testConfig = testConfig;
        this.jwtSupplier = jwtSupplier;

        thingsWebsocketClient = null;
    }

    public static ThingsWebSocketClientResource newInstance(final TestConfig testConfig,
            final AccessTokenSupplier<JsonWebToken> jwtSupplier) {

        return new ThingsWebSocketClientResource(checkNotNull(testConfig, "testConfig"),
                checkNotNull(jwtSupplier, "jwtSupplier"));
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        thingsWebsocketClient = createThingsWebsocketClient();
        connect(thingsWebsocketClient);
    }

    private ThingsWebsocketClient createThingsWebsocketClient() {
        // Mock basicAuth parameter as no acceptance tests uses this method
        final BasicAuth basicAuth = BasicAuth.newInstance(false, "", "");

        return ThingsWebSocketClientFactory.getWebSocketClient(testConfig, jwtSupplier, basicAuth, DittoHeaders.empty());
    }

    private static void connect(final ThingsWebsocketClient thingsWebsocketClient) {
        final var connectCorrelationId = CorrelationId.random();
        LOGGER.info("Connecting {} with correlation ID <{}>>",
                ThingsWebsocketClient.class.getSimpleName(),
                connectCorrelationId);
        thingsWebsocketClient.connect(connectCorrelationId);
    }

    @Override
    protected void after() {
        if (null != thingsWebsocketClient) {
            try {
                thingsWebsocketClient.close();
            } finally {
                LOGGER.info("Closed {}.", ThingsWebsocketClient.class.getSimpleName());
            }
        }
        super.after();
    }

    public ThingsWebsocketClient getThingsWebsocketClient() {
        if (null == thingsWebsocketClient) {
            final var pattern = "{0} gets only initialised by running the test.";
            throw new IllegalStateException(MessageFormat.format(pattern, ThingsWebsocketClient.class.getSimpleName()));
        } else {
            return thingsWebsocketClient;
        }
    }

}
