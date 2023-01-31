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
package org.eclipse.ditto.testing.system.search.security;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchProperties;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.junit.Test;

/**
 * Tests Authentication of the Search REST API.
 */
public final class AuthenticationIT extends VersionedSearchIntegrationTest {

    private static final SearchFilter KNOWN_FILTER = SearchProperties.thingId().eq("knownValue");

    @Test
    public void validAuthentication() {
        searchThings()
                .filter(KNOWN_FILTER)
                .fire();
    }

    @Test
    public void missingAuthenticationForSearchThings() {
        searchThings()
                .clearAuthentication()
                .filter(KNOWN_FILTER)
                .expectingHttpStatus(HttpStatus.UNAUTHORIZED)
                .fire();
    }

    @Test
    public void invalidAuthenticationForSearchThings() {
        searchThings()
                .withJWT("invalid Token")
                .filter(KNOWN_FILTER)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

}
