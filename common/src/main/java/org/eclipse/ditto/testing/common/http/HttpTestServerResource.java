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

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.internal.utils.akka.logging.DittoLogger;
import org.eclipse.ditto.internal.utils.akka.logging.DittoLoggerFactory;
import org.eclipse.ditto.testing.common.junit.ExternalResourceState;
import org.junit.rules.ExternalResource;

/**
 * This external resources creates and provides a {@link HttpTestServer}.
 */
@NotThreadSafe
public final class HttpTestServerResource extends ExternalResource {

    private static final DittoLogger LOGGER = DittoLoggerFactory.getLogger(HttpTestServerResource.class);

    private HttpTestServer httpTestServer;
    private ExternalResourceState state;

    private HttpTestServerResource() {
        httpTestServer = null;
        state = ExternalResourceState.INITIAL;
    }

    /**
     * Returns a new instance of {@code HttpTestServerResource}.
     *
     * @return the instance.
     */
    public static HttpTestServerResource newInstance() {
        return new HttpTestServerResource();
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        httpTestServer = HttpTestServer.newInstance();
        state = ExternalResourceState.POST_BEFORE;
    }

    @Override
    protected void after() {
        try {
            httpTestServer.close();
        } catch (final Exception e) {
            if (LOGGER.isWarnEnabled()) {
                final var pattern = "Failed to close the HTTP server with reason: {1}";
                LOGGER.warn(MessageFormat.format(pattern, e.getMessage()), e);
            }
        }
        state = ExternalResourceState.POST_AFTER;
        super.after();
    }

    public HttpTestServer getHttpTestServer() {
        if (ExternalResourceState.POST_BEFORE == state) {
            return httpTestServer;
        } else if (ExternalResourceState.INITIAL == state) {
            throw new IllegalStateException("The HTTP test server gets only available with running a test.");
        } else {
            throw new IllegalStateException("The HTTP test server was already closed and cannot be used anymore.");
        }
    }

    public int getHttpTestServerPort() {
        final var httpServer = getHttpTestServer();
        return httpServer.getPort();
    }

}
