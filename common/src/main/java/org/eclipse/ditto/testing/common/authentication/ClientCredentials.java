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

import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;

/**
 * Client credentials are a pair consisting of a client ID and a client secret.
 * They are required for retrieving an access token via OAuth 2.0's client credentials flow.
 */
@Immutable
final class ClientCredentials {

    private final String clientId;
    private final String clientSecret;

    private ClientCredentials(final String clientId, final String clientSecret) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    /**
     * Returns an instance of {@code ClientCredentials} that wraps the specified arguments.
     *
     * @param clientId the client ID.
     * @param clientSecret the client secret.
     * @return the instance.
     * @throws NullPointerException if any argument is {@code null}.
     * @throws IllegalArgumentException if any argument is empty.
     */
    public static ClientCredentials of(final CharSequence clientId, final CharSequence clientSecret) {
        ConditionChecker.argumentNotEmpty(clientId, "clientId");
        ConditionChecker.argumentNotEmpty(clientSecret, "clientSecret");

        return new ClientCredentials(clientId.toString(), clientSecret.toString());
    }

    public String getClientId() {
        return clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    @Override
    public boolean equals(@Nullable final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final var that = (ClientCredentials) o;
        return Objects.equals(clientId, that.clientId) && Objects.equals(clientSecret, that.clientSecret);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, clientSecret);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [" +
                "clientId=" + clientId +
                ", clientSecret=" + "*****" +
                "]";
    }

}
