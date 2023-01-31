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
package org.eclipse.ditto.testing.common.conditions;

import static java.util.Objects.requireNonNull;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.MessageFormat;

import javax.annotation.Nullable;

import org.junit.AssumptionViolatedException;
import org.junit.rules.MethodRule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Annotatable;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

public final class ConditionalRunRule implements TestRule, MethodRule {

    @Override
    public Statement apply(final Statement base, final FrameworkMethod method, final Object target) {
        requireNonNull(base);
        requireNonNull(method);
        requireNonNull(target);

        Statement result = base;

        final Condition classCondition = getRunIfCondition(target.getClass());
        if (classCondition != null && !classCondition.isSatisfied()) {
            result = new IgnoreStatement(classCondition, target.getClass());
        }

        final Condition methodCondition = getRunIfCondition(method);
        if (methodCondition != null && !methodCondition.isSatisfied()) {
            result = new IgnoreStatement(methodCondition, method);
        }

        return result;
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
        final Condition condition = getRunIfCondition(description);
        if (condition != null && !condition.isSatisfied()) {
            return new IgnoreStatement(condition, description);
        }

        return base;
    }

    @Nullable
    private static Condition getRunIfCondition(final AnnotatedElement annotatedElement) {
        final RunIf annotation = annotatedElement.getAnnotation(RunIf.class);

        return createConditionIfAnnotationExists(annotation);
    }

    @Nullable
    private static Condition getRunIfCondition(final Annotatable method) {
        final RunIf annotation = method.getAnnotation(RunIf.class);

        return createConditionIfAnnotationExists(annotation);
    }

    @Nullable
    private Condition getRunIfCondition(final Description description) {
        final RunIf annotation = description.getAnnotation(RunIf.class);

        return createConditionIfAnnotationExists(annotation);
    }


    private static Condition createConditionIfAnnotationExists(@Nullable final RunIf annotation) {
        if (annotation == null) {
            return null;
        }

        return new ConditionCreator<>(annotation.value()).create();
    }

    private static class IgnoreStatement extends Statement {
        private static final String IGNORED_MESSAGE_TEMPLATE = "Ignored Test: ''{0}''. Reason: Condition ''{1}'' was" +
                " not fulfilled.";
        private final Condition condition;
        private final Object ignoredObject;

        private IgnoreStatement(final Condition condition, final Object ignoredObject) {
            this.condition = requireNonNull(condition);
            this.ignoredObject = requireNonNull(ignoredObject);
        }

        @Override
        public void evaluate() {
            final String msg = MessageFormat.format(IGNORED_MESSAGE_TEMPLATE,
                    ignoredObject, condition.getClass().getSimpleName());
            throw new AssumptionViolatedException(msg);
        }
    }

    private static final class ConditionCreator<T> {

        private static final String ILLEGAL_CONDITION_MESSAGE_TEMPLATE =
                "Conditional class ''{0}'' is a member class. Member classes are not supported.";

        private final Class<T> conditionType;

        private ConditionCreator(final Class<T> conditionType) {
            this.conditionType = requireNonNull(conditionType);
        }

        private T create() {
            checkConditionType();
            try {
                return tryToCreateCondition();
            } catch (final RuntimeException re) {
                throw re;
            } catch (final Exception e) {
                throw new IllegalStateException(e);
            }
        }

        private T tryToCreateCondition() throws InstantiationException, NoSuchMethodException,
                InvocationTargetException, IllegalAccessException {
            try {
               return createCondition();
            } catch (final IllegalAccessException iae) {
                final Constructor<T> constructor = conditionType.getDeclaredConstructor();
                constructor.setAccessible(true);
                return constructor.newInstance();
            }
        }

        private T createCondition() throws IllegalAccessException, InstantiationException {
            return conditionType.newInstance();
        }

        private void checkConditionType() {
            if (conditionType.isMemberClass()) {
                final String msg = MessageFormat.format(ILLEGAL_CONDITION_MESSAGE_TEMPLATE, conditionType.getName());
                throw new IllegalArgumentException(msg);
            }
        }

    }
}
