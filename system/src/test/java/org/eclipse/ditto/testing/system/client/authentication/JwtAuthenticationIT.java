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
package org.eclipse.ditto.testing.system.client.authentication;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.eclipse.ditto.client.DittoClient;
import org.eclipse.ditto.client.configuration.AccessTokenAuthenticationConfiguration;
import org.eclipse.ditto.client.configuration.BasicAuthenticationConfiguration;
import org.eclipse.ditto.client.configuration.ProxyConfiguration;
import org.eclipse.ditto.client.messaging.AuthenticationProviders;
import org.eclipse.ditto.client.messaging.JsonWebTokenSupplier;
import org.eclipse.ditto.jwt.model.ImmutableJsonWebToken;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.client.BasicAuth;
import org.eclipse.ditto.testing.common.client.oauth.AuthClient;
import org.eclipse.ditto.testing.system.client.AbstractClientIT;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests Client JWT Authentication.
 */
public final class JwtAuthenticationIT extends AbstractClientIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(JwtAuthenticationIT.class);

    private DittoClient dittoClient;
    private ThingId thingId;

    @Before
    public void setUp() {
        LOGGER.info("-----> Running Test: {}", name.getMethodName());

        thingId = ThingId.of(serviceEnv.getDefaultNamespaceName(),
                "testThingForJwtAuthentication-" + UUID.randomUUID());
    }

    @After
    public void tearDown() {
        if (dittoClient != null) {
            try {
                dittoClient.twin().delete(thingId).toCompletableFuture().get(TIMEOUT_SECONDS, SECONDS);
                dittoClient.policies().delete(PolicyId.of(thingId));
            } catch (final Exception e) {
                LOGGER.error("Error during Test", e);
            } finally {
                shutdownClient(dittoClient);
            }
        }
    }

    @Category({Acceptance.class})
    @Test
    public void authentication() throws ExecutionException, InterruptedException {
        if (serviceEnv.getDefaultTestingContext().getBasicAuth().isEnabled()) {
            dittoClient = buildClient(serviceEnv.getDefaultTestingContext().getBasicAuth());
        } else {
            dittoClient = buildClient(() -> {
                final AuthClient authClient = serviceEnv.getDefaultTestingContext().getOAuthClient();
                return ImmutableJsonWebToken.fromToken(authClient.getAccessToken());
            });
        }
        final Thing thing = dittoClient.twin().create(thingId).toCompletableFuture().get();
        assertThat(thing).isNotNull();
    }

    private static DittoClient buildClient(final JsonWebTokenSupplier jsonWebTokenSupplier) {
        final AccessTokenAuthenticationConfiguration.AccessTokenAuthenticationConfigurationBuilder
                configurationBuilder = AccessTokenAuthenticationConfiguration.newBuilder()
                .identifier("client:" + UUID.randomUUID())
                .accessTokenSupplier(jsonWebTokenSupplier);
        final ProxyConfiguration proxyConfiguration = proxyConfiguration();
        if (null != proxyConfiguration) {
            configurationBuilder.proxyConfiguration(proxyConfiguration);
        }
        return newDittoClient(AuthenticationProviders.accessToken(configurationBuilder.build()),
                Collections.emptyList(), Collections.emptyList());
    }

    private static DittoClient buildClient(final BasicAuth basicAuth) {
        final BasicAuthenticationConfiguration.BasicAuthenticationConfigurationBuilder configurationBuilder =
                BasicAuthenticationConfiguration.newBuilder()
                .username(basicAuth.getUsername())
                .password(basicAuth.getPassword());
        if (null != proxyConfiguration()) {
            configurationBuilder.proxyConfiguration(proxyConfiguration());
        }
        return newDittoClient(AuthenticationProviders.basic(configurationBuilder.build()),
                Collections.emptyList(), Collections.emptyList());
    }

}
