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

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;

/**
 * Definition of a well-known option for the Ditto Protocol client.
 */
@Immutable
public final class OptionDefinition<T> {

    private final OptionName optionName;
    private final Class<T> valueType;

    private OptionDefinition(final OptionName optionName, final Class<T> valueType) {
        this.optionName = optionName;
        this.valueType = valueType;
    }

    public static <T> OptionDefinition<T> newInstance(final OptionName optionName, final Class<T> valueType) {
        return new OptionDefinition<>(ConditionChecker.checkNotNull(optionName, "optionName"),
                ConditionChecker.checkNotNull(valueType, "valueType"));
    }

    public OptionName getName() {
        return optionName;
    }

    public Class<T> getValueType() {
        return valueType;
    }

    @Override
    public boolean equals(@Nullable final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final var that = (OptionDefinition<?>) o;
        return Objects.equals(optionName, that.optionName) && Objects.equals(valueType, that.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(optionName, valueType);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [" +
                "optionName=" + optionName +
                ", valueType=" + valueType +
                "]";
    }

}
