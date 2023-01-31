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
package org.eclipse.ditto.testing.common;

import static java.util.Objects.requireNonNull;

import java.util.Random;

/**
 * Combines some useful string methods.
 */
public final class StringUtils {

    private StringUtils() {

    }

    /**
     * Create and return a string with random content with a specified length.
     *
     * @param length how long the string should be
     * @param seed the initial seed
     * @return random string
     */
    public static String createRandomString(final int length, final long seed) {
        final StringBuilder sb = new StringBuilder();

        new Random(seed).ints(length, 'a', ('z') + 1).forEach(number -> //
        {
            sb.append((char) number);
        });

        if (seed != 0) {
            sb.append(seed);
        }

        return sb.toString();
    }

    /**
     * Checks whether the given string is empty, i.e. {@code null} or of length zero.
     *
     * @param string the string
     * @return {@code true}, if empty
     */
    public static boolean isEmpty(final String string) {
        return (string == null) || (string.length() == 0);
    }

    /**
     * Left pads the given string with n characters.
     *
     * @param string the string
     * @param size the size to which should be padded
     * @param padString the string to be used for padding
     * @return the left padded string
     */
    public static String padLeft(final String string, final int size, final String padString) {
        checkPadLeftParams(string, size, padString);

        final int padLength = padString.length();
        final int strLength = string.length();
        final int padsCount = size - strLength;
        if (padsCount == 0) {
            return string;
        } else if (padsCount == padLength) {
            return padString.concat(string);
        } else if (padsCount < padLength) {
            return padString.substring(0, padsCount).concat(string);
        }

        final char[] paddingChars = createPaddingChars(padString, padsCount);
        return new String(paddingChars).concat(string);
    }

    private static void checkPadLeftParams(final String string, final int size, final String padString) {
        requireNonNull(string);
        requireNonNull(padString);
        if (isEmpty(padString)) {
            throw new IllegalArgumentException("padString must not be empty");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size must be >= 0");
        }
    }

    private static char[] createPaddingChars(final String padString, final int padsCount) {
        final int padLength = padString.length();
        final char[] paddingChars = new char[padsCount];
        final char[] padChars = padString.toCharArray();
        for (int i = 0; i < padsCount; i++) {
            paddingChars[i] = padChars[i % padLength];
        }
        return paddingChars;
    }

}
