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

import org.junit.rules.TestRule;

/**
 * Defines a {@link org.junit.rules.TestRule} that provides a test solution.
 */
public interface TestSolutionSupplierRule extends TestRule {

    /**
     * Returns the solution under test.
     *
     * @return the solution under test.
     * @throws IllegalStateException if this method is called before the test was run.
     */
    Solution getTestSolution();

    /**
     * Returns the ID of the test solution.
     *
     * @return the ID of the test solution.
     * @throws IllegalStateException if this method was called before a test was run.
     */
    default String getTestUsername() {
        final var testSolution = getTestSolution();
        return testSolution.getUsername();
    }

}
