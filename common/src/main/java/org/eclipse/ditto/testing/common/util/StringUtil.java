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
package org.eclipse.ditto.testing.common.util;

import java.text.MessageFormat;

import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;

/**
 * Utility functions for Strings.
 */
@Immutable
public final class StringUtil {

    private static final int DEFAULT_TRUNCATED_STRING_LENGTH = 300;

    private StringUtil() {
        throw new AssertionError();
    }

    /**
     * Truncates the specified string argument to the default length of
     * {@value #DEFAULT_TRUNCATED_STRING_LENGTH} characters.
     *
     * @param s the string to be truncated.
     * @return the truncated string or {@code s} if the default length is greater than or equal the length of {@code s}.
     * @throws NullPointerException if {@code s} is {@code null}.
     */
    public static String truncate(final String s) {
        return truncate(s, DEFAULT_TRUNCATED_STRING_LENGTH);
    }

    /**
     * Truncates the specified string argument to the specified length argument.
     *
     * @param s the string to be truncated.
     * @param length the length of the returned truncated string.
     * @return the truncated string or {@code s} if {@code length} is greater than or equal the length of {@code s}.
     * @throws NullPointerException if {@code s} is {@code null}.
     * @throws IllegalArgumentException if {@code s} is less than zero.
     */
    public static String truncate(final String s, final int length) {
        ConditionChecker.checkNotNull(s, "s");
        ConditionChecker.checkArgument(length,
                i -> 0 < i,
                () -> MessageFormat.format("Expected length to be greater than zero but it was <{0,number,integer}>",
                        length));

        final String result;
        if (s.isEmpty()) {
            result = s;
        } else if (length >= s.length()) {
            result = s;
        } else {
            result = s.substring(0, length) + "…";
        }
        return result;
    }

}
