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

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for checking that a Json Object is empty.
 */
public final class EmptyJsonMatcher extends TypeSafeMatcher<String> {

    @Override
    protected boolean matchesSafely(final String actualJsonString) {
        final com.eclipsesource.json.JsonValue jsonValue = com.eclipsesource.json.Json.parse(actualJsonString);
        return jsonValue.isObject() && jsonValue.asObject().isEmpty();
    }

    @Override
    public void describeTo(final Description description) {

    }
}
