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
package org.eclipse.ditto.testing.system.search.things;

import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.and;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.not;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.testing.common.SearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchProperties;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.junit.Test;

/**
 * Tests that "not" is allowed for API/1 and disallowed for API/2.
 */
public class QueryThingsWithNegationIT extends SearchIntegrationTest {

    private static final SearchFilter NEGATED_FILTER = and(
            not(SearchProperties.thingId().exists()),
            not(SearchProperties.attribute("unexpectedAttribute").exists())
    );

    @Test
    public void negationIsAllowedForAPI_2() {
        final HttpStatus expectedStatusCode = HttpStatus.OK;

        searchThings(JsonSchemaVersion.V_2)
                .filter(NEGATED_FILTER)
                .expectingHttpStatus(expectedStatusCode)
                .fire();
    }
}
