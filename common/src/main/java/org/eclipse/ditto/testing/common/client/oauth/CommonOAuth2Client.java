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
package org.eclipse.ditto.testing.common.client.oauth;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.util.Optional;

import javax.annotation.Nullable;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.eclipse.ditto.base.model.json.FieldType;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonFieldDefinition;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.jwt.model.ImmutableJsonWebToken;
import org.eclipse.ditto.jwt.model.JsonWebToken;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonOAuth2Client {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommonOAuth2Client.class);

    private static final JsonFieldDefinition<String> ACCESS_TOKEN =
            JsonFactory.newStringFieldDefinition("access_token", FieldType.REGULAR);

    static final String GRANT_TYPE_KEY = "grant_type";
    static final String CLIENT_ID_KEY = "client_id";
    static final String CLIENT_SECRET_KEY = "client_secret";
    static final String SCOPE_KEY = "scope";
    static final String CONTENT_TYPE = "application/x-www-form-urlencoded";
    static final String GRANT_TYPE = "client_credentials";

    private final String tokenEndpoint;
    private final String clientId;
    private final String clientSecret;
    @Nullable private final String scope;
    private final AsyncHttpClient httpClient;

    private String accessToken = null;
    private JsonWebToken jsonWebToken;

    CommonOAuth2Client(final String tokenEndpoint,
            final String clientId,
            final String clientSecret,
            @Nullable final String scope,
            final AsyncHttpClient httpClient) {

        this.tokenEndpoint = tokenEndpoint;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.scope = scope;
        this.httpClient = httpClient;
    }

    public static CommonOAuth2Client newInstance(final String tokenEndpoint,
            final String clientId,
            final String clientSecret,
            final String scope,
            final AsyncHttpClient httpClient) {

        checkNotNull(tokenEndpoint, "tokenEndpoint");
        checkNotNull(clientId, "clientId");
        checkNotNull(clientSecret, "clientSecret");
        checkNotNull(httpClient, "httpClient");

        return new CommonOAuth2Client(tokenEndpoint, clientId, clientSecret, scope, httpClient);
    }

    public String getTokenEndpoint() {
        return tokenEndpoint;
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public Optional<String> getScope() {
        return Optional.ofNullable(scope);
    }

    public String getAccessToken() {
        if (accessToken == null || (jsonWebToken != null && jsonWebToken.isExpired())) {
            accessToken = retrieveAccessToken();
            jsonWebToken = ImmutableJsonWebToken.fromToken(accessToken);
        }
        return accessToken;
    }

    private String retrieveAccessToken() {
        LOGGER.info("Retrieving access token for oauth 2 client from endpoint <{}>", tokenEndpoint);
        final BoundRequestBuilder request = httpClient.preparePost(tokenEndpoint)
                .addHeader(HttpHeader.CONTENT_TYPE.getName(), CONTENT_TYPE)
                .addFormParam(CLIENT_ID_KEY, clientId)
                .addFormParam(CLIENT_SECRET_KEY, clientSecret)
                .addFormParam(GRANT_TYPE_KEY, GRANT_TYPE);

        if (scope != null) {
            request.addFormParam(SCOPE_KEY, scope);
        }

        final ListenableFuture<Response> execute = request.execute();
        final Response response = execute.toCompletableFuture().join();
        final JsonObject body = JsonFactory.newObject(response.getResponseBody());
        LOGGER.debug("Response Status Code for getting the Access Token was: {}", response.getStatusCode());
        LOGGER.debug("Response Body: {}", body);

        return body.getValue(ACCESS_TOKEN)
                .orElseThrow(() -> new IllegalStateException("Failed to retrieve access token from Body: " + body));
    }
}
