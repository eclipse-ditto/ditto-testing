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

import org.apache.http.HttpStatus;
import org.eclipse.ditto.internal.utils.akka.logging.DittoLogger;
import org.eclipse.ditto.internal.utils.akka.logging.DittoLoggerFactory;
import org.eclipse.ditto.jwt.model.JsonWebToken;
import org.eclipse.ditto.testing.common.restassured.UnexpectedResponseException;

/**
 * A client for our OAuth mock back-end.
 */
@Immutable
public final class OAuthMockClient {

    private static final DittoLogger LOGGER = DittoLoggerFactory.getLogger(OAuthMockClient.class);

    private final OAuthClient oAuthClient;

    private OAuthMockClient(final OAuthClient oAuthClient) {
        this.oAuthClient = oAuthClient;
    }

    /**
     * Returns a new instance of {@code OAuthMockClient}.
     *
     * @param oAuthMockTokenUri URI of the token endpoint.
     * @return the instance.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public static OAuthMockClient newInstance(final URI oAuthMockTokenUri) {

        try {
            return new OAuthMockClient(
                    OAuthClient.newInstance(checkNotNull(oAuthMockTokenUri, "oAuthMockTokenUri"))
            );
        } finally {
            LOGGER.info("Initialised for token endpoint <{}>.", oAuthMockTokenUri);
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
    public JsonWebToken getAccessToken(final ClientCredentials clientCredentials, final CharSequence scope) {
        return oAuthClient.getAccessToken(clientCredentials, scope);
    }

}
