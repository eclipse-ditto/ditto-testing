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
import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for checking that a Json Object only contains the specified Json keys.
 */
public final class JsonKeyStrictMatcher extends TypeSafeMatcher<String> {

    private final List<String> expectedJsonKeys;
    private final boolean recursive;

    /**
     * Constructs a new {@code JsonKeyStrictMatcher} object.
     *
     * @param expectedJsonKeys the only keys a JSON Object is expected to contain.
     * @throws NullPointerException if {@code expectedJsonKeys} is {@code null}.
     */
    public JsonKeyStrictMatcher(final List<String> expectedJsonKeys, final boolean recursive) {
        this.expectedJsonKeys = requireNonNull(expectedJsonKeys, "The expected keys must not be null!");
        this.recursive = recursive;
    }


    @Override
    protected boolean matchesSafely(final String actualJsonString) {
        final com.eclipsesource.json.JsonValue jsonValue = com.eclipsesource.json.Json.parse(actualJsonString);
        return containsExpectedKeysOnly(jsonValue);
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("a JSON object containing only keys ").appendValueList("{", ",", "}", expectedJsonKeys);
    }

    private boolean containsExpectedKeysOnly(final com.eclipsesource.json.JsonValue jsonValue) {
        boolean result = true;

        if (jsonValue.isObject()) {
            result = containsExpectedKeysOnly((com.eclipsesource.json.JsonObject) jsonValue);
        } else if (jsonValue.isArray()) {
            for (final com.eclipsesource.json.JsonValue jsonValueMember : ((com.eclipsesource.json.JsonArray) jsonValue)
                    .values()) {
                if (!jsonValueMember.isObject()) {
                    result = false;
                    break;
                } else {
                    result &= containsExpectedKeysOnly(jsonValueMember);
                }
            }
        } else {
            result = expectedJsonKeys.size() == 1 && jsonValue.toString().equals(expectedJsonKeys.get(0));
        }

        return result;
    }

    private boolean containsExpectedKeysOnly(final com.eclipsesource.json.JsonObject jsonObject) {
        final List<String> actualFieldNames = getAllObjectMemberNames(jsonObject);

        boolean result = actualFieldNames.size() == expectedJsonKeys.size();
        if (result) {
            for (final String expectedAttribute : expectedJsonKeys) {
                result &= actualFieldNames.contains(expectedAttribute);
            }
        }
        return result;
    }

    private List<String> getAllObjectMemberNames(final com.eclipsesource.json.JsonObject jsonObject) {
        final List<String> names = new ArrayList<>();

        jsonObject.iterator().forEachRemaining(member -> {
            if (member.getValue().isObject()) {
                names.add(member.getName());
                if (recursive) {
                    names.addAll(getAllObjectMemberNames(member.getValue().asObject()));
                }
            } else {
                names.add(member.getName());
            }
        });

        return names;
    }
}
