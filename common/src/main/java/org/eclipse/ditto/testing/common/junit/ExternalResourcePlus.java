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

import java.util.ArrayList;
import java.util.List;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

/**
 * This {@code TestRule} behaves exactly like {@link org.junit.rules.ExternalResource} with one difference:
 * In {@link #before(Description)}} and {@link #after(Description)} the {@link Description} of the test is available.
 * This allows to implement advanced use cases where, for example, annotation processing is involved.
 */
public abstract class ExternalResourcePlus implements TestRule {

    protected ExternalResourcePlus() {
        super();
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
        return getStatement(base, description);
    }

    private Statement getStatement(final Statement base, final Description description) {
        return new MyStatement(description, base);
    }

    /**
     * Override to set up your specific external resource.
     * This method does nothing by default.
     *
     * @param description a {@code Description} of the test implemented in the statement to be modified.
     * @throws Throwable if setup fails. This will disable {@link #after(Description)}.
     */
    @SuppressWarnings("java:S112")
    protected void before(final Description description) throws Throwable {
        // does nothing by default
    }

    /**
     * Override to tear down your specific external resource.
     * This method does nothing by default.
     *
     * @param description a {@code Description} of the test implemented in the statement to be modified.
     */
    @SuppressWarnings("java:S112")
    protected void after(final Description description) {
        // does nothing by default
    }

    private final class MyStatement extends Statement {

        private final Description description;
        private final Statement base;
        private final List<Throwable> errors;

        public MyStatement(final Description description, final Statement base) {
            this.description = description;
            this.base = base;
            errors = new ArrayList<>(3);
        }

        @Override
        public void evaluate() throws Throwable {
            before(description);

            try {
                tryToEvaluate();
            } finally {
                tryToTearDown();
            }
            MultipleFailureException.assertEmpty(errors);
        }

        private void tryToEvaluate() {
            try {
                base.evaluate();
            } catch (final Throwable t) {
                errors.add(t);
            }
        }

        @SuppressWarnings("java:S1181")
        private void tryToTearDown() {
            try {
                after(description);
            } catch (final Throwable t) {
                errors.add(t);
            }
        }

    }

}
