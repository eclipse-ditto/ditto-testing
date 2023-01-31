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
package org.eclipse.ditto.testing.common.assertions;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.asynchttpclient.Response;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.assertions.DittoJsonAssertions;

/**
 * An assert for {@link Response}.
 */
public class AhcResponseAssert extends AbstractAssert<AhcResponseAssert, Response> {

    /**
     * Constructs a new {@code AhcResponseAssert} object.
     *
     * @param actual the response to be checked.
     */
    public AhcResponseAssert(final Response actual) {
        super(actual, AhcResponseAssert.class);
    }

    public AhcResponseAssert hasJsonObjectBody(final String expectedBody) {
        return hasJsonObjectBody(JsonFactory.newObject(expectedBody));
    }

    public AhcResponseAssert hasJsonObjectBody(final JsonObject expectedBody) {
        isNotNull();
        final String actualResponseBodyString = actual.getResponseBody();
        Assertions.assertThat(actualResponseBodyString).isNotEmpty();
        final JsonObject actualResponseBodyJsonObject = JsonFactory.newObject(actualResponseBodyString);
        DittoJsonAssertions.assertThat(actualResponseBodyJsonObject)
                .overridingErrorMessage("Expected response body to be\n<%s> but it was\n<%s>", expectedBody,
                        actualResponseBodyJsonObject)
                .isEqualTo(expectedBody);
        return myself;
    }

}
