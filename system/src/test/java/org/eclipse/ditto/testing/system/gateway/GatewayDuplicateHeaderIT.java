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
package org.eclipse.ditto.testing.system.gateway;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ExecutionException;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.testing.common.client.http.AsyncHttpClientFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration test for duplicate header fields.
 */
public class GatewayDuplicateHeaderIT extends IntegrationTest {

    private AsyncHttpClient client;

    @Before
    public void setup() {
        client = AsyncHttpClientFactory.newInstance(TEST_CONFIG);
    }

    @After
    public void teardown() {
        AsyncHttpClientFactory.close(client);
    }

    @Test
    @Category(Acceptance.class)
    public void postThingWithDuplicateCorrelationIdHeader() throws ExecutionException, InterruptedException {

        final String url = thingsServiceUrl(TestConstants.API_V_2, TestConstants.Things.THINGS_PATH);

        final ListenableFuture<Response> execute = client.preparePost(url)
                .setHeader("Authorization",
                        "Bearer " + serviceEnv.getDefaultTestingContext().getOAuthClient().getAccessToken())
                .addHeader("Cache-Control", "no-cache")
                .addHeader("x-correlation-id", "correlation-id-1")
                .addHeader("x-correlation-id", "correlation-id-2")
                .execute();

        final Response response = execute.get();
        final JsonObject body = JsonFactory.newObject(response.getResponseBody());
        LOGGER.info("postThingWithDuplicateCorrelationIdHeader response: {}", response);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST.getCode());
        assertThat(body.getValue("error")).contains(JsonValue.of("gateway:duplicate.header.field"));
    }

}
