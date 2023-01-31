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

import java.util.function.Consumer;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher which allows arbitrary assertions implemented in a {@link java.util.function.Consumer}.
 * <p>Inspired by assertJ's method {@link org.assertj.core.api.AbstractAssert#satisfies(Consumer)}.</p>
 */
public final class SatisfiesMatcher<T> extends TypeSafeMatcher<T> {

    private final Consumer<T> requirements;

    /**
     * Constructs a new {@link SatisfiesMatcher} object.
     *
     * @param requirements the consumer containing the assertions
     */
    public SatisfiesMatcher(final Consumer<T> requirements) {
        this.requirements = requireNonNull(requirements);
    }

    @Override
    public boolean matchesSafely(final T item) {
        // requirements should fail with AssertionError in case of failing assert
        try {
            requirements.accept(item);
        } catch (final AssertionError ae) {
            /*
              Make sure that request/response is logged.
              (workaround for https://github.com/rest-assured/rest-assured/issues/1129)
              To avoid side effects with awaitility, Awaitility#ignoreExceptionsByDefault() has to be called once on
              test startup.
            */
            throw new AssertionErrorWrappingException(ae);
        }

        return true;
    }

    @Override
    public void describeTo(final Description description) {
    }

    private static final class AssertionErrorWrappingException extends RuntimeException {

        private AssertionErrorWrappingException(final AssertionError wrapped) {
            super(requireNonNull(wrapped));
        }
    }
}
