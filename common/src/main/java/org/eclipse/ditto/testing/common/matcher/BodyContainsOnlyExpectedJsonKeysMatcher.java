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

import java.util.List;
import java.util.stream.Collectors;

import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonFieldDefinition;
import org.eclipse.ditto.json.JsonKey;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for checking if a JSON string contains only the expected fields.
 */
public final class BodyContainsOnlyExpectedJsonKeysMatcher extends TypeSafeMatcher<String> {

    private final List<? extends JsonFieldDefinition> expectedJsonFieldDefinitions;

    /**
     * Constructs a new {@code BodyContainsOnlyExpectedJsonKeysMatcher} object.
     *
     * @param expectedJsonFieldDefinitions the only fields a JSON string is expected to contain.
     * @throws NullPointerException if {@code expectedJsonFieldDefinitions} is {@code null}.
     */
    public BodyContainsOnlyExpectedJsonKeysMatcher(
            final List<? extends JsonFieldDefinition> expectedJsonFieldDefinitions) {
        this.expectedJsonFieldDefinitions =
                requireNonNull(expectedJsonFieldDefinitions, "The expected JSON fiels must not be null!");
    }

    @Override
    protected boolean matchesSafely(final String actualJsonString) {
        final JsonValue jsonValue = JsonFactory.readFrom(actualJsonString);
        return containsExpectedFieldsOnly(jsonValue);
    }

    private boolean containsExpectedFieldsOnly(final JsonValue jsonValue) {
        boolean result = true;

        if (jsonValue.isObject()) {
            result = containsExpectedFieldsOnly((JsonObject) jsonValue);
        } else if (jsonValue.isArray()) {
            for (final JsonValue jsonValueMember : jsonValue.asArray()) {
                if (!jsonValueMember.isObject()) {
                    result = false;
                    break;
                } else {
                    result &= containsExpectedFieldsOnly(jsonValueMember);
                }
            }
        } else {
            result = false;
        }

        return result;
    }

    private boolean containsExpectedFieldsOnly(final JsonObject jsonObject) {
        final List<String> actualFieldNames =
                jsonObject.getKeys().stream().map(JsonKey::toString).collect(Collectors.toList());
        boolean result = actualFieldNames.size() == expectedJsonFieldDefinitions.size();
        if (result) {
            for (final JsonFieldDefinition expectedJsonFieldDefinition : expectedJsonFieldDefinitions) {
                result &= actualFieldNames.contains(
                        expectedJsonFieldDefinition.getPointer().toString().replaceFirst("/", ""));
            }
        }
        return result;
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("a JSON string containing only ").appendValueList("{", ",", "}",
                expectedJsonFieldDefinitions);
    }
}
