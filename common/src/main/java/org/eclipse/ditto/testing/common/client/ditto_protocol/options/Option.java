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

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.util.Objects;

/**
 * An Option can be used to control the behaviour of the Ditto Protocol client.
 * This class might be mutable because it wraps an unknown value which can be mutable.
 *
 * @param <T> the type of the wrapped option value.
 */
public final class Option<T> {

    private final OptionName name;
    private final T value;

    private Option(final OptionName name, final T value) {
        this.name = name;
        this.value = value;
    }

    /**
     * Returns a new instance of {@code Option} for the specified arguments.
     *
     * @param optionDefinition the definition of the returned option.
     * @param optionValue the value of the returned option.
     * @param <T> the type of the option value.
     * @return the new instance.
     * @throws NullPointerException if any argument is {@code null}.
     */
    public static <T> Option<T> newInstance(final OptionDefinition<T> optionDefinition, final T optionValue) {
        checkNotNull(optionDefinition, "optionDefinition");

        return new Option<>(optionDefinition.getName(), checkNotNull(optionValue, "optionValue"));
    }

    /**
     * Returns the name of this option.
     *
     * @return the name of this option.
     */
    public OptionName getName() {
        return name;
    }

    /**
     * Returns the plain wrapped option value.
     *
     * @return the wrapped value.
     */
    public T getValue() {
        return value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final var that = (Option<?>) o;
        return Objects.equals(name, that.name) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " [" + "name=" + name + ", value=" + value + "]";
    }

}
