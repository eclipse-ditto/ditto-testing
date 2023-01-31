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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonKey;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * This matcher checks if the JSON response body of a HTTP request contains the specified JSON keys.
 */
public final class BodyContainsJsonKeysMatcher extends TypeSafeMatcher<String> {

    private final List<JsonKey> expectedJsonKeys;

    /**
     * Constructs a new {@code BodyContainsJsonKeysMatcher} object.
     *
     * @param theExpectedJsonKeys the JSON keys which are expected to be contained in the JSON response body.
     * @throws NullPointerException if {@code theExpectedJsonKeys} is {@code null}.
     */
    public BodyContainsJsonKeysMatcher(final Set<JsonKey> theExpectedJsonKeys) {
        requireNonNull(theExpectedJsonKeys, "The expected JSON keys must not be null!");

        expectedJsonKeys = Collections.unmodifiableList(new ArrayList<>(theExpectedJsonKeys));
    }

    @Override
    protected boolean matchesSafely(final String actualJsonString) {
        final JsonValue jsonValue = JsonFactory.readFrom(actualJsonString);

        return containsExpectedKeysAmongOthers(jsonValue);
    }

    private boolean containsExpectedKeysAmongOthers(final JsonValue jsonValue) {
        boolean result = true;

        if (jsonValue.isObject()) {
            final JsonObject jsonObject = jsonValue.asObject();
            final List<JsonKey> actualKeys = jsonObject.getKeys();
            result = actualKeys.containsAll(expectedJsonKeys);
        } else if (jsonValue.isArray()) {
            for (final JsonValue value : jsonValue.asArray()) {
                result &= containsExpectedKeysAmongOthers(value);
            }
        } else if (1 == expectedJsonKeys.size()) {
            final JsonKey expectedJsonKey = expectedJsonKeys.get(0);
            result = expectedJsonKey.equals(jsonValue.toString());
        } else {
            result = false;
        }

        return result;
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("a JSON object containing the keys ").appendValueList("{", ",", "}", expectedJsonKeys)
                .appendText(" among others");
    }
}
