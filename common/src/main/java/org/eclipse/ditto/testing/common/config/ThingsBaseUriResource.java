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
package org.eclipse.ditto.testing.common.config;

import java.net.URI;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.testing.common.gateway.GatewayConfig;
import org.junit.rules.ExternalResource;

/**
 * Provides the base URI for the Things service as defined by test configuration.
 */
@NotThreadSafe
public final class ThingsBaseUriResource extends ExternalResource {

    private static final String THINGS_RESOURCE_PATH = "./things";

    private final GatewayConfig gatewayConfig;

    @Nullable private URI thingsBaseUriApi2;

    private ThingsBaseUriResource(final GatewayConfig gatewayConfig) {
        this.gatewayConfig = gatewayConfig;
        thingsBaseUriApi2 = null;
    }

    /**
     * Returns a new instance of {@code ThingsBaseUriResource}.
     *
     * @param testConfig the test configuration properties.
     * @return the instance.
     * @throws NullPointerException if {@code testConfig} is {@code null}.
     */
    public static ThingsBaseUriResource newInstance(final TestConfig testConfig) {
        return new ThingsBaseUriResource(GatewayConfig.of(testConfig));
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        thingsBaseUriApi2 = resolveThingsBaseUriApi2();
    }

    private URI resolveThingsBaseUriApi2() {
        final var httpUriApi2 = gatewayConfig.getHttpUriApi2();
        return httpUriApi2.resolve(THINGS_RESOURCE_PATH);
    }

    public URI getThingsBaseUriApi2() {
        if (null == thingsBaseUriApi2) {
            throw new IllegalStateException("The Things base URI for API 2 is only available after running the test.");
        } else {
            return thingsBaseUriApi2;
        }
    }

}
