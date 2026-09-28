/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
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
package org.eclipse.ditto.testing.system.gateway;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.testing.common.*;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assume.assumeFalse;

/**
 * Integration test for the authorization with Devops basic auth at the API Gateway.
 */
public final class GatewayDevopsBasicAuthorizationIT extends IntegrationTest {

    private static final String RANDOM_NAMESPACE = ServiceEnvironment.createRandomDefaultNamespace();
    private final CommonTestConfig testConfig;

    public GatewayDevopsBasicAuthorizationIT() {
        this.testConfig = CommonTestConfig.getInstance();
    }

    @Test
    @Category(Acceptance.class)
    public void retrieveAllConnectionIds() {
        // WHEN
        if (testConfig.isDevopsAuthEnabled()) {
            connectionsClient()
                    .getConnectionIds()
                    .withBasicAuth(testConfig.getDevopsAuthUser(), testConfig.getDevopsAuthPassword())
                    .expectingHttpStatus(HttpStatus.OK)
                    .fire();
        } else {
            LOGGER.info("Devops auth is disabled. Test skipped.");
        }
    }

}