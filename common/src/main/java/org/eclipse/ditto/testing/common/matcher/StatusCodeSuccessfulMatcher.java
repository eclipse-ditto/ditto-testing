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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Consumer;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * Checks whether a given status code is in the OK range.
 */
public final class StatusCodeSuccessfulMatcher extends TypeSafeMatcher<Integer> {

    /**
     * Assert that HTTP status code is in the OK range (at least 200, at most 299).
     */
    private static final Consumer<Integer> CONSUMER = code ->
            assertThat(code).matches(StatusCodeSuccessfulMatcher::isHttpStatusSuccessful,
                    "is successful http status");

    private static final StatusCodeSuccessfulMatcher INSTANCE = new StatusCodeSuccessfulMatcher();

    private final SatisfiesMatcher<Integer> delegate;

    private StatusCodeSuccessfulMatcher() {
        delegate = new SatisfiesMatcher<>(CONSUMER);
    }

    /**
     * Returns the single instance of this matcher.
     *
     * @return the instance
     */
    public static StatusCodeSuccessfulMatcher getInstance() {
        return INSTANCE;
    }

    /**
     * Returns a consumer for using this matcher with AssertJ.
     *
     * @return the consumer
     */
    public static Consumer<Integer> getConsumer() {
        return CONSUMER;
    }

    @Override
    protected boolean matchesSafely(final Integer statusCode) {
        return delegate.matchesSafely(statusCode);
    }

    @Override
    public void describeTo(final Description description) {
        // not needed
    }

    /**
     * Returns whether the given HTTP status code is in the OK range (at least 200, at most 299).
     *
     * @param statusCode the actual status code.
     */
    public static boolean isHttpStatusSuccessful(final int statusCode) {
        return HttpStatus.tryGetInstance(statusCode).filter(HttpStatus::isSuccess).isPresent();
    }

}
