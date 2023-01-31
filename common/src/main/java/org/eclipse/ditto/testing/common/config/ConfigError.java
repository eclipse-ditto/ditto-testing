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

import javax.annotation.Nullable;

/**
 * This error is thrown to indicate that a config property did not match an expectation.
 * Details are provided in detail message and the optional cause.
 */
public class ConfigError extends Error {

    private static final long serialVersionUID = 3237098543235773858L;

    /**
     * Constructs a {@code ConfigError} object.
     *
     * @param message the detail message of the error.
     */
    public ConfigError(final String message) {
        super(message);
    }

    /**
     * Constructs a {@code ConfigError} object.
     *
     * @param cause the cause of the error.
     */
    public ConfigError(final Throwable cause) {
        super(cause);
    }

    /**
     * Constructs a {@code ConfigError} object.
     *
     * @param message the detail message of the error.
     * @param cause the cause of the error or {@code null} if unknown.
     */
    public ConfigError(final String message, @Nullable final Throwable cause) {
        super(message, cause);
    }

}
