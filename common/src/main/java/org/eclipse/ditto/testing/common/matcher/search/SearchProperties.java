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
package org.eclipse.ditto.testing.common.matcher.search;

import static java.util.Objects.requireNonNull;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.property;

import org.eclipse.ditto.thingsearch.model.SearchProperty;

/**
 * Contains factory methods to create common instances of type {@link SearchProperty}.
 */
public final class SearchProperties {

    private static final SearchProperty THING_ID = property(PropertyPaths.thingId());

    private SearchProperties() {
        throw new AssertionError();
    }

    public static SearchProperty thingId() {
        return THING_ID;
    }

    public static SearchProperty attribute(final String key) {
        requireNonNull(key);

        return property(PropertyPaths.attribute(key));
    }

    public static SearchProperty created() {
        return property(PropertyPaths.created());
    }

    public static SearchProperty modified() {
        return property(PropertyPaths.modified());
    }

    public static SearchProperty definition() {
        return property(PropertyPaths.definition());
    }

    public static SearchProperty featureProperty(final String key) {
        requireNonNull(key);

        return property(PropertyPaths.featureProperty(key));
    }

    public static SearchProperty featureProperty(final String featureId, final String key) {
        requireNonNull(featureId);
        requireNonNull(key);

        return property(PropertyPaths.featureProperty(featureId, key));
    }

    public static SearchProperty featureDesiredProperty(final String featureId, final String key) {
        requireNonNull(featureId);
        requireNonNull(key);

        return property(PropertyPaths.featureDesiredProperty(featureId, key));
    }

    public static SearchProperty featureDesiredProperty(final String featureId) {
        requireNonNull(featureId);

        return property(PropertyPaths.featureDesiredProperty(featureId));
    }

    public static SearchProperty feature(final String subPath) {
        requireNonNull(subPath);

        return property(PropertyPaths.feature(subPath));
    }

}
