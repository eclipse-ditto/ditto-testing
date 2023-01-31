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
import java.util.Optional;

import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for checking equality of one ore more expected Thing IDs.
 */
public final class ThingsIdsExactlyMatcher extends TypeSafeMatcher<String> {

    private final List<String> expectedThingsIds;

    /**
     * Constructs a new {@code ThingsIdsMatcher} object.
     *
     * @param expectedThingsIds the list of the expected Things IDs.
     * @throws NullPointerException if {@code expectedThingsIds} is {@code null}.
     */
    public ThingsIdsExactlyMatcher(final List<String> expectedThingsIds) {
        this.expectedThingsIds = requireNonNull(expectedThingsIds);
    }

    @Override
    public boolean matchesSafely(final String responseBody) {
        boolean result;
        if (responseBody == null || responseBody.isEmpty()) {
            result = false;
        } else {
            final JsonArray jsonArray = JsonFactory.newArray(responseBody);
            final int jsonArraySize = jsonArray.getSize();
            result = jsonArraySize == expectedThingsIds.size();
            if (!result) {
                return false;
            }
            for (int i = 0; i < jsonArraySize; i++) {
                final JsonValue thingJson = jsonArray.get(i).get();
                final JsonObject thingJsonObject = thingJson.asObject();
                final Optional<JsonValue> thingId = thingJsonObject.getValue("thingId");
                final String actualThingId = thingId.map(JsonValue::asString).orElse("");
                if (!actualThingId.equals(expectedThingsIds.get(i))) {
                    result = false;
                    break;
                }
            }
        }
        return result;
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("The Thing ID(s) should be").appendValue(expectedThingsIds);
    }
}
