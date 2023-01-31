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

public final class CharSequenceContainsMatcher extends TypeSafeMatcher<String> {
    private final CharSequence charSequence;

    public CharSequenceContainsMatcher(final CharSequence charSequence) {
        this.charSequence = charSequence;
    }

    @Override
    protected boolean matchesSafely(final String s) {
        return s != null && s.contains(this.charSequence);
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText("Char sequence \"" + this.charSequence + "\" not found.");
    }
}
