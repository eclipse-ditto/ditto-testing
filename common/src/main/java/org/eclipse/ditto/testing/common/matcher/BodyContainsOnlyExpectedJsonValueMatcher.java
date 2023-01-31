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

import static java.util.Objects.requireNonNull;

import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonRuntimeException;
import org.eclipse.ditto.json.JsonValue;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for checking if a JSON string contains only the expected fields.
 */
public final class BodyContainsOnlyExpectedJsonValueMatcher extends TypeSafeMatcher<String> {

    private final JsonValue jsonValue;

    public BodyContainsOnlyExpectedJsonValueMatcher(final JsonValue jsonValue) {
        this.jsonValue = requireNonNull(jsonValue, "The expected JSON value must not be null!");
    }

    @Override
    protected boolean matchesSafely(final String expectedJsonString) {
        try {
            // parse this.jsonValue again to avoid different java classes representing the same Json
            final JsonValue canonicalJsonValue = JsonFactory.readFrom(jsonValue.toString());
            final JsonValue expectedJsonValue = JsonFactory.readFrom(expectedJsonString);
            return canonicalJsonValue.equals(expectedJsonValue);
        } catch (final JsonRuntimeException e) {
            return false;
        }
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("a JSON value containing only ").appendText(jsonValue.toString());
    }
}
