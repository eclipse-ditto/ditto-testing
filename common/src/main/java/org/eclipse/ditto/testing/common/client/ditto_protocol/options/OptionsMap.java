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

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.eclipse.ditto.base.model.common.ConditionChecker;

/**
 * Provides access to user specified {@link Option}s by their {@link OptionDefinition}.
 */
@Immutable
public final class OptionsMap {

    private final Map<OptionName, Option<?>> optionsByOptionName;

    private OptionsMap(final Map<OptionName, Option<?>> optionsByOptionName) {
        this.optionsByOptionName = optionsByOptionName;
    }

     public static OptionsMap of(final Option<?>[] userProvidedOptions) {
        return new OptionsMap(
                Stream.of(ConditionChecker.checkNotNull(userProvidedOptions, "userProvidedOptions"))
                        .collect(Collectors.toUnmodifiableMap(Option::getName, Function.identity()))
        );
    }

     public <T> Optional<T> getValue(final OptionDefinition<T> optionDefinition) {
        ConditionChecker.checkNotNull(optionDefinition, "optionDefinition");

        final Optional<T> result;
        @Nullable final var rawOption = optionsByOptionName.get(optionDefinition.getName());
        if (null != rawOption) {
            final var valueType = optionDefinition.getValueType();
            result = Optional.of(valueType.cast(rawOption.getValue()));
        } else {
            result = Optional.empty();
        }
        return result;
    }

}
