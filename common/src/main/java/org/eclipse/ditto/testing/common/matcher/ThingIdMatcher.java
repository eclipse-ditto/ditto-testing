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
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for checking equality of one ore more expected Thing IDs.
 */
public final class ThingIdMatcher extends TypeSafeMatcher<String> {

    private final ThingId expectedThingId;

    /**
     * Constructs a new {@code ThingIdMatcher} object.
     *
     * @param theExpectedThingId the the expected Thing ID.
     * @throws NullPointerException if {@code theExpectedThingId} is {@code null}.
     */
    public ThingIdMatcher(final ThingId theExpectedThingId) {
        expectedThingId = requireNonNull(theExpectedThingId);
    }

    @Override
    public boolean matchesSafely(final String responseBody) {
        if (responseBody == null || responseBody.isEmpty()) {
            return false;
        }
        final JsonObject jsonObject = JsonFactory.newObject(responseBody);
        return expectedThingId.equals(jsonObject.getValue(Thing.JsonFields.ID)
                .map(ThingId::of)
                .orElse(null));
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("The Thing ID should be").appendValue(expectedThingId);
    }
}
