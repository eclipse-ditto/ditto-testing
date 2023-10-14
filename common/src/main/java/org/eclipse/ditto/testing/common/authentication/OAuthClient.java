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
package org.eclipse.ditto.testing.common.authentication;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.net.URI;

import javax.annotation.concurrent.Immutable;

import org.apache.commons.codec.Charsets;
import org.apache.http.HttpStatus;
import org.eclipse.ditto.base.model.common.ConditionChecker;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLogger;
import org.eclipse.ditto.internal.utils.pekko.logging.DittoLoggerFactory;
import org.eclipse.ditto.jwt.model.ImmutableJsonWebToken;
import org.eclipse.ditto.jwt.model.JsonWebToken;
import org.eclipse.ditto.testing.common.correlationid.CorrelationId;
import org.eclipse.ditto.testing.common.restassured.UnexpectedResponseException;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.response.Response;

/**
 * A client for the OAuth HTTP API.
 */
@Immutable
final class OAuthClient {

    private static final DittoLogger LOGGER = DittoLoggerFactory.getLogger(OAuthClient.class);

    private final String tokenUri;

    private OAuthClient(final URI tokenUri) {
        this.tokenUri = tokenUri.toString();
    }

    /**
     * Returns a new instance of {@code OAuthClient}.
     *
     * @param oAuthTokenEndpointUri URI of the auth token endpoint to be used by the returned client.
     * @return the instance.
     * @throws NullPointerException if {@code oAuthTokenEndpointUri} is {@code null}.
     */
    static OAuthClient newInstance(final URI oAuthTokenEndpointUri) {
        try {
            return new OAuthClient(checkNotNull(oAuthTokenEndpointUri, "oAuthTokenEndpointUri"));
        } finally {
            LOGGER.info("Initialised for token endpoint <{}>.", oAuthTokenEndpointUri);
        }
    }

    /**
     * Gets the access token as {@link JsonWebToken} from the authorization server's token endpoint of this client.
     *
     * @param clientCredentials the client ID and client secret to request an access token for from
     * authorization server.
     * @param scope a non-empty custom scope for requesting an access token from authorization server.
     * @return the access token.
     * @throws NullPointerException if any argument is {@code null}.
     * @throws IllegalArgumentException if {@code scope} is empty.
     * @throws UnexpectedResponseException.WrongStatusCode if the response has a different status code than
     * {@link HttpStatus#SC_OK}.
     */
    JsonWebToken getAccessToken(final ClientCredentials clientCredentials, final CharSequence scope) {
        checkNotNull(clientCredentials, "clientCredentials");
        ConditionChecker.argumentNotEmpty(scope, "scope");
        final var correlationId = CorrelationId.random().withSuffix(".retrieveBearerToken");

        LOGGER.withCorrelationId(correlationId);
        LOGGER.info("Retrieving access token for client ID <{}> and scope <{}> …",
                clientCredentials.getClientId(),
                scope);
        final var response = RestAssured
                .given().
                baseUri(tokenUri).
                contentType(ContentType.URLENC.withCharset(Charsets.UTF_8)).
                param("grant_type", "client_credentials").
                param("client_id", clientCredentials.getClientId()).
                param("client_secret", clientCredentials.getClientSecret()).
                param("scope", scope).
                header(correlationId.toHeader())
                .when().
                post();

        try {
            final var expectedStatusCode = HttpStatus.SC_OK;
            if (response.getStatusCode() == expectedStatusCode) {
                LOGGER.info("Got access token for client ID <{}>.", clientCredentials.getClientId());
                return getJsonWebToken(response);
            } else {
                throw new UnexpectedResponseException.WrongStatusCode(expectedStatusCode, response);
            }
        } finally {
            LOGGER.discardCorrelationId();
        }
    }

    private static JsonWebToken getJsonWebToken(final Response response) {
        final var responseBody = response.getBody();
        final var jsonPath = responseBody.jsonPath();
        return ImmutableJsonWebToken.fromToken(jsonPath.getString("access_token"));
    }

}
