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
import org.eclipse.ditto.things.model.ThingId;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for checking equality of one or more expected Thing IDs.
 */
public final class ThingsIdsMatcher extends TypeSafeMatcher<String> {

    private final List<ThingId> expectedThingsIds;
    private final List<ThingId> notExpectedThingsIds;

    /**
     * Constructs a new {@code ThingsIdsMatcher} object.
     *
     * @param expectedThingsIds the list of the expected Things IDs.
     * @param notExpectedThingsIds the list of the not expected Things IDs.
     * @throws NullPointerException if {@code expectedThingsIds} is {@code null}.
     */
    public ThingsIdsMatcher(final List<ThingId> expectedThingsIds, final List<ThingId> notExpectedThingsIds) {
        this.expectedThingsIds = requireNonNull(expectedThingsIds);
        this.notExpectedThingsIds = requireNonNull(notExpectedThingsIds);
    }

    @Override
    public boolean matchesSafely(final String responseBody) {
        final boolean result;
        if (responseBody == null || responseBody.isEmpty()) {
            result = false;
        } else {
            final JsonArray jsonArray = JsonFactory.newArray(responseBody);
            int expectedThingsCount = 0;
            int notExpectedThingsCount = 0;

            for (int i = 0; i < jsonArray.getSize(); i++) {
                final JsonValue thingJson = jsonArray.get(i).get();
                final JsonObject thingJsonObject = thingJson.asObject();
                final Optional<JsonValue> thingId = thingJsonObject.getValue("thingId");
                final ThingId actualThingId = thingId.map(JsonValue::asString).map(ThingId::of).orElse(null);
                if (expectedThingsIds.contains(actualThingId)) {
                    expectedThingsCount++;
                }
                if (notExpectedThingsIds.contains(actualThingId)) {
                    notExpectedThingsCount++;
                }
            }
            result =
                    (!expectedThingsIds.isEmpty() && expectedThingsCount == expectedThingsIds.size())
                            || (!notExpectedThingsIds.isEmpty() && notExpectedThingsCount == 0);
        }
        return result;
    }

    @Override
    public void describeTo(final Description description) {
        if (!expectedThingsIds.isEmpty()) {
            description.appendText("The Thing ID(s) should be").appendValue(expectedThingsIds);
        } else {
            description.appendText("The Thing ID(s) should not be").appendValue(notExpectedThingsIds);
        }
    }
}
