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

import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for checking if the JSON of a response expectingBody is an empty array (collection).
 */
public final class EmptyCollectionMatcher extends TypeSafeMatcher<String> {

    @Override
    protected boolean matchesSafely(final String responseBody) {
        final JsonArray jsonArray = JsonFactory.newArray(responseBody);
        return jsonArray.isEmpty();
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("The collection should be empty");
    }
}
