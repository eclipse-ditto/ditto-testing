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
import java.util.Collection;
import java.util.Optional;

import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonField;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Checks if the response body of a HTTP request contains an expected JSON value.
 */
public class BodyContainsJsonValueMatcher extends TypeSafeMatcher<String> {

    private final JsonValue expectedJsonValue;

    /**
     * Constructs a new {@code BodyContainsJsonValueMatcher} object.
     *
     * @param theExpectedJsonValue the JSON value the response body is expected to contain.
     * @throws NullPointerException if {@code theExpectedJsonValue} is {@code null}.
     */
    public BodyContainsJsonValueMatcher(final JsonValue theExpectedJsonValue) {
        expectedJsonValue = requireNonNull(theExpectedJsonValue, "The expected JSON value must not be null!");
    }

    @Override
    protected boolean matchesSafely(final String actualJsonString) {
        if (actualJsonString == null || actualJsonString.equals("")) {
            return false;
        }

        final JsonValue jsonValue = JsonFactory.readFrom(actualJsonString);
        return contains(jsonValue);
    }

    private boolean contains(final JsonValue jsonValue) {
        boolean result = false;

        if (expectedJsonValue.equals(jsonValue)) {
            result = true;
        } else if (expectedJsonValue.isNull() && jsonValue.isNull()) {
            result = true;
        } else if (expectedJsonValue.isObject()) {
            final ActualContainsAllOfJsonObjectChecker c = new ActualContainsAllOfJsonObjectChecker(jsonValue);
            result = c.contains(expectedJsonValue.asObject());
        } else if (expectedJsonValue.isArray()) {
            final ActualContainsAllOfJsonArrayChecker c = new ActualContainsAllOfJsonArrayChecker(jsonValue);
            result = c.contains(expectedJsonValue.asArray());
        }

        return result;
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("a JSON string containing ").appendValue(expectedJsonValue).appendText(" among others");
    }

    private static final class ActualContainsAllOfJsonObjectChecker {

        final JsonValue actualJsonValue;

        public ActualContainsAllOfJsonObjectChecker(final JsonValue theActualJsonValue) {
            actualJsonValue = theActualJsonValue;
        }

        public boolean contains(final JsonObject expected) {
            boolean result;

            if (actualJsonValue.isNull()) {
                result = expected.isNull();
            } else {
                result = !expected.isNull();
            }

            if (actualJsonValue.isObject()) {
                final JsonObject actualJsonObject = actualJsonValue.asObject();
                for (final JsonField expectedJsonField : expected) {
                    final Optional<JsonValue> actualValue = actualJsonObject.getValue(expectedJsonField.getKey());
                    if (actualValue.isPresent()) {
                        final JsonValue jsonValue = actualValue.get();
                        if (jsonValue.isObject()) {
                            // recurse!
                            final ActualContainsAllOfJsonObjectChecker c =
                                    new ActualContainsAllOfJsonObjectChecker(jsonValue);
                            result &= c.contains(expectedJsonField.getValue().asObject());
                        } else if (jsonValue.isArray()) {
                            result &= actualValue.filter(JsonValue::isArray)
                                    .map(JsonValue::asArray)
                                    .map(ActualContainsAllOfJsonArrayChecker::new)
                                    .map(checker -> checker.contains(jsonValue.asArray()))
                                    .orElse(Boolean.FALSE);
                        } else {
                            final JsonValue expectedValue = expectedJsonField.getValue();
                            result &= actualValue.map(expectedValue::equals).orElse(false);
                        }
                    } else {
                        result = false;
                    }
                }
            } else if (actualJsonValue.isArray()) {
                final JsonArray actualJsonArray = actualJsonValue.asArray();
                for (final JsonField expectedJsonField : expected) {
                    result &= actualJsonArray.contains(expectedJsonField.getValue());
                }
            } else {
                result = false;
            }

            return result;
        }
    }

    private static final class ActualContainsAllOfJsonArrayChecker {

        final JsonValue actualJsonValue;

        public ActualContainsAllOfJsonArrayChecker(final JsonValue theActualJsonValue) {
            actualJsonValue = theActualJsonValue;
        }

        public boolean contains(final JsonArray expected) {
            boolean result;

            if (actualJsonValue.isNull()) {
                result = expected.isNull();
            } else {
                result = !expected.isNull();
            }

            if (actualJsonValue.isObject()) {
                final JsonObject actualJsonObject = actualJsonValue.asObject();
                final Collection<JsonValue> actualJsonValues = new ArrayList<>();
                for (final JsonField actualJsonField : actualJsonObject) {
                    actualJsonValues.add(actualJsonField.getValue());
                }
                for (final JsonValue expectedJsonValue : expected) {
                    result &= actualJsonValues.contains(expectedJsonValue);
                }
            } else if (actualJsonValue.isArray()) {
                final JsonArray actualJsonArray = actualJsonValue.asArray();
                for (final JsonValue expectedJsonValue : expected) {
                    result &= actualJsonArray.contains(expectedJsonValue);
                }
            } else {
                result = false;
            }

            return result;
        }
    }
}
