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
package org.eclipse.ditto.testing.common.junit;

/**
 * An enumeration of frequently occurring states of {@link org.junit.rules.ExternalResource} and alike.
 */
public enum ExternalResourceState {

    /**
     * The test was not yet run.
     * The {@code ExternalResource} is in its initial state.
     */
    INITIAL,

    /**
     * The method {@code before()} was executed.
     */
    POST_BEFORE,

    /**
     * The method {@code after()} was executed.
     */
    POST_AFTER

}
