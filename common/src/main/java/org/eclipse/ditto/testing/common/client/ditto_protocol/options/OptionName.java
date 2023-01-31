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
package org.eclipse.ditto.testing.common.client.ditto_protocol.options;

import java.util.Objects;
import java.util.function.Predicate;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;

/**
 * The name of an Option.
 */
@Immutable
public final class OptionName {

    private final String optionNameValue;

    private OptionName(final String optionNameValue) {
        this.optionNameValue = optionNameValue;
    }

    /**
     * Returns an instance of {@code OptionName} for the given CharSequence argument.
     *
     * @param optionNameValue the value of the returned option name.
     * @return the instance.
     * @throws NullPointerException if {@code optionNameValue} is {@code null}.
     * @throws IllegalArgumentException if {@code optionNameValue} is blank.
     */
    public static OptionName of(final CharSequence optionNameValue) {
        ConditionChecker.checkNotNull(optionNameValue, "optionNameValue");
        final var optionNameString = optionNameValue.toString();
        ConditionChecker.checkArgument(optionNameString,
                Predicate.not(String::isBlank),
                () -> "The optionNameValue must not be blank.");
        return new OptionName(optionNameString);
    }

    @Override
    public boolean equals(@Nullable final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final var that = (OptionName) o;
        return Objects.equals(optionNameValue, that.optionNameValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(optionNameValue);
    }

    @Override
    public String toString() {
        return optionNameValue;
    }

}
