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
package org.eclipse.ditto.testing.common.matcher;

import java.util.Optional;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonField;
import org.eclipse.ditto.json.JsonValue;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

@Immutable
public final class BodyContainsErrorCodeMatcher extends TypeSafeMatcher<String> {

    private final String expectedErrorCode;

    public BodyContainsErrorCodeMatcher(final String expectedErrorCode) {
        this.expectedErrorCode = expectedErrorCode;
    }

    @Override
    protected boolean matchesSafely(final String actualJsonString) {
        final JsonValue jsonValue = JsonFactory.readFrom(actualJsonString);
        final Optional<JsonField> errorField = jsonValue.asObject().getField("error");

        if (errorField.isPresent()) {
            final String actualErrorCode = errorField.get().getValue().formatAsString();
            return actualErrorCode.equals(expectedErrorCode);
        }

        return false;
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("Response object containing a field with key 'error' and value '")
                .appendValue(expectedErrorCode)
                .appendText("'.");
    }
}
