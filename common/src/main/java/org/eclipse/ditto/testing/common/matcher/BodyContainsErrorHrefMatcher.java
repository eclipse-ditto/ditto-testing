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

import java.util.Optional;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonField;
import org.eclipse.ditto.json.JsonValue;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

@Immutable
public final class BodyContainsErrorHrefMatcher extends TypeSafeMatcher<String> {

    private final Matcher<String> matchingFunction;

    public BodyContainsErrorHrefMatcher(final Matcher<String> matcher) {
        this.matchingFunction = matcher;
    }

    public BodyContainsErrorHrefMatcher(final String expectedErrorHref) {
        this(CoreMatchers.equalTo(expectedErrorHref));
    }

    @Override
    protected boolean matchesSafely(final String actualJsonString) {
        final JsonValue jsonValue = JsonFactory.readFrom(actualJsonString);
        final Optional<JsonField> errorField = jsonValue.asObject().getField("href");

        if (errorField.isPresent()) {
            final String actualErrorCode = errorField.get().getValue().formatAsString();
            return matchingFunction.matches(actualErrorCode);
        }

        return false;
    }

    @Override
    public void describeTo(final Description description) {
        matchingFunction.describeTo(description);
    }
}
