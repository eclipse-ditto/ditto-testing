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

/**
 * This interface defines functionality to get an OAuth 2.0 access token.
 *
 * @param <T> the type of the access token this client retrieves from the authorization server.
 */
public interface AccessTokenSupplier<T> {

    /**
     * Returns the retrieved access token from the authorization server.
     *
     * @return the retrieved access token from the authorization server.
     */
    T getAccessToken();

}
