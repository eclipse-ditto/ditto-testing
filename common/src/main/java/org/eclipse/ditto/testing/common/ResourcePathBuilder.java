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

import static java.util.Objects.requireNonNull;
import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A mutable builder with a fluent API for creating Strings of HTTP resource paths for the various components of
 * Ditto.
 */
@NotThreadSafe
public final class ResourcePathBuilder {

    private static final int INITIAL_SIZE = 256;
    private static final char SLASH = '/';

    private final StringBuilder stringBuilder;

    private ResourcePathBuilder() {
        stringBuilder = new StringBuilder(INITIAL_SIZE);
    }

    /**
     * Returns a path builder for a Thing resource.
     *
     * @param thingId the ID of the Thing.
     * @return the builder.
     * @throws NullPointerException if {@code thingId} is {@code null}.
     */
    public static Thing forThing(final CharSequence thingId) {
        return new ResourcePathBuilder().new Thing(thingId);
    }

    /**
     * Returns a path builder for a Policy resource.
     *
     * @param policyId the ID of the Policy.
     * @return the builder.
     * @throws NullPointerException if {@code policyId} is {@code null}.
     */
    public static Policy forPolicy(final CharSequence policyId) {
        return new ResourcePathBuilder().new Policy(requireNonNull(policyId));
    }

    /**
     * Returns a path builder for a Connection resource.
     *
     * @param connectionId the ID of the Solution.
     * @return the builder.
     * @throws NullPointerException if {@code connectionId} is {@code null}.
     */
    public static Connection forConnection(final CharSequence connectionId) {
        return new ResourcePathBuilder().new Connections().connection(connectionId);
    }

    /**
     * Returns a path builder for the CheckPermissions resource.
     *
     * @return the builder.
     */
    public static CheckPermissions forCheckPermissions() {
        return new ResourcePathBuilder().new CheckPermissions();
    }

    /**
     * Inner class to build paths for CheckPermissions.
     */
    public final class CheckPermissions extends AbstractPathBuilder {

        private CheckPermissions() {
            stringBuilder.append(HttpResource.CHECK_PERMISSIONS);
        }

        @Override
        public String toString() {
            return stringBuilder.toString();
        }
    }

    /**
     * Returns a path builder for all Connections.
     *
     * @return the builder.
     */
    public static Connections connections() {
        return new ResourcePathBuilder().new Connections();
    }

    private abstract class AbstractPathBuilder {

        protected AbstractPathBuilder() {
            super();
        }

        @Override
        public String toString() {
            return stringBuilder.toString();
        }

    }

    public final class Thing extends AbstractPathBuilder {

        private Thing(final CharSequence thingId) {
            stringBuilder.append(HttpResource.THINGS).append(SLASH).append(checkNotNull(thingId, "Thing ID"));
        }

        public Thing policyId() {
            stringBuilder.append(HttpResource.POLICY_ID);
            return this;
        }

        public Thing attributes() {
            stringBuilder.append(HttpResource.ATTRIBUTES);
            return this;
        }

        public Thing attribute(final CharSequence attributePointer) {
            attributes();
            stringBuilder.append(SLASH).append(checkNotNull(attributePointer, "Attribute pointer"));
            return this;
        }

        public Thing features() {
            stringBuilder.append(HttpResource.FEATURES);
            return this;
        }

        public Thing feature(final CharSequence featureId) {
            features();
            stringBuilder.append(SLASH).append(checkNotNull(featureId, "Feature ID"));
            return this;
        }

        public Thing properties() {
            stringBuilder.append(HttpResource.PROPERTIES);
            return this;
        }

        public Thing desiredProperties() {
            stringBuilder.append(HttpResource.DESIRED_PROPERTIES);
            return this;
        }

        public Thing property(final CharSequence propertyPointer) {
            properties();
            stringBuilder.append(SLASH).append(checkNotNull(propertyPointer, "propertyPointer"));
            return this;
        }

        public Thing desiredProperty(final CharSequence desiredPropertyPointer) {
            desiredProperties();
            stringBuilder.append(SLASH).append(checkNotNull(desiredPropertyPointer, "desiredPropertyPointer"));
            return this;
        }

        public Thing definition() {
            stringBuilder.append(HttpResource.DEFINITION);
            return this;
        }

        public Thing migrateDefinition() {
            stringBuilder.append(HttpResource.MIGRATE_DEFINITION);
            return this;
        }

    }

    public final class Policy extends AbstractPathBuilder {

        private Policy(final CharSequence policyId) {
            stringBuilder.append(HttpResource.POLICIES).append(SLASH).append(policyId);
        }

        public Policy subject(final String label, final String subjectId) {
            subjects(label);
            stringBuilder.append(SLASH).append(subjectId);
            return this;
        }

        public Policy subjects(final String label) {
            policyEntry(label);
            stringBuilder.append(HttpResource.SUBJECTS);
            return this;
        }

        public Policy resources(final String label) {
            policyEntry(label);
            stringBuilder.append(HttpResource.RESOURCES);
            return this;
        }

        public Policy resource(final String label, final String resourcePath) {
            resources(label);
            stringBuilder.append(SLASH).append(resourcePath);
            return this;
        }

        public Policy policyEntry(final CharSequence label) {
            checkNotNull(label, "PolicyEntry label");
            policyEntries();
            stringBuilder.append(SLASH).append(label);
            return this;
        }

        public Policy policyEntries() {
            stringBuilder.append(SLASH).append("entries");
            return this;
        }

        public Policy policyImports() {
            stringBuilder.append(SLASH).append("imports");
            return this;
        }

        public Policy policyImport(final CharSequence importedPolicyId) {
            stringBuilder.append(SLASH).append("imports").append(SLASH).append(importedPolicyId);
            return this;
        }

        public Policy action(final String label, final String actionName) {
            policyEntries();
            stringBuilder.append(SLASH).append(label);
            action(actionName);
            return this;
        }

        public Policy action(final String actionName) {
            stringBuilder.append(HttpResource.ACTIONS).append(SLASH).append(actionName);
            return this;
        }
    }

    public final class Connections extends AbstractPathBuilder {

        private Connections() {
            stringBuilder.append(HttpResource.CONNECTIONS);
        }

        public Connection connection(final CharSequence connectionId) {
            return new Connection(connectionId);
        }
    }

    public final class Connection extends AbstractPathBuilder {

        private Connection(final CharSequence connectionId) {
            stringBuilder.append(SLASH).append(checkNotNull(connectionId, "the connection ID"));
        }

        public Connection logs() {
            stringBuilder.append(HttpResource.LOGS);
            return this;
        }

        public Connection command() {
            stringBuilder.append(HttpResource.COMMAND);
            return this;
        }

        public Connection status() {
            stringBuilder.append(HttpResource.STATUS);
            return this;
        }

    }

}
