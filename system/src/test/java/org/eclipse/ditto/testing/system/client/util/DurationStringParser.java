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
package org.eclipse.ditto.testing.system.client.util;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.Immutable;

/**
 * This class parses a String to a {@link java.time.Duration}.
 */
@Immutable
final class DurationStringParser implements Supplier<Duration> {

    private static final Pattern DURATION_TAG_OPEN = Pattern.compile("<duration>", Pattern.LITERAL);
    private static final Pattern DURATION_TAG_CLOSE = Pattern.compile("</duration>", Pattern.LITERAL);

    private final CharSequence durationString;

    private DurationStringParser(final CharSequence durationString) {
        this.durationString = durationString;
    }

    /**
     * Returns a new instance of {@code DurationStringParser}.
     *
     * @param byteBuffer provides the bytes for the string to be parsed to a Duration.
     * @param charset the Charset to be used for creating a String from {@code byteBuffer}.
     * @return the instance.
     * @throws NullPointerException if any argument is {@code null}.
     */
    static DurationStringParser getInstance(final ByteBuffer byteBuffer, final Charset charset) {
        checkNotNull(byteBuffer, "byte buffer");
        checkNotNull(charset, "charset");

        return new DurationStringParser(createDurationString(byteBuffer, charset));
    }

    private static CharSequence createDurationString(final ByteBuffer byteBuffer, final Charset charset) {
        String result = new String(byteBuffer.array(), charset);
        result = DURATION_TAG_OPEN.matcher(result).replaceAll(Matcher.quoteReplacement(""));
        result = DURATION_TAG_CLOSE.matcher(result).replaceAll(Matcher.quoteReplacement(""));

        return result;
    }

    @Override
    public Duration get() {
        return Duration.parse(durationString);
    }

}
