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
package org.eclipse.ditto.testing.common.config;

import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.util.Optional;

/**
 * An enumeration of supported test environments.
 */
public enum TestEnvironment {

    DOCKER_COMPOSE("docker-compose"),

    LOCAL("local");

    private final String configValue;

    TestEnvironment(final String configValue) {
        this.configValue = configValue;
    }

    /**
     * Returns the {@code TestEnvironment} that has the same string representation as the specified char sequence
     * argument.
     *
     * @param cs the string representation of the {@code TestEnvironment} to be found.
     * @return an Optional containing the found {@code TestEnvironment} or an empty optional if {@code cs} is unknown.
     * @throws NullPointerException if {@code cs} is {@code null}.
     */
    public static Optional<TestEnvironment> getForString(final String cs) {
        checkNotNull(cs, "cs");
        for (final var testEnvironment : values()) {
            if (cs.startsWith(testEnvironment.configValue)) {
                return Optional.of(testEnvironment);
            }
        }
        return Optional.empty();
    }

    /**
     * Returns the string representation of this {@code TestEnvironment}.
     *
     * @return the lower case name of this {@code Environment} where underscore is replaced by dash {@code "-"} as
     * delimiter.
     */
    @Override
    public String toString() {
        return configValue;
    }

}
