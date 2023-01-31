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

/**
 * Contains factory methods to create common property paths.
 */
final class PropertyPaths {

    private static final String THING_ID = "thingId";
    private static final String ATTRIBUTES_PREFIX = "attributes/";
    private static final String CREATED = "_created";
    private static final String MODIFIED = "_modified";
    private static final String DEFINITION = "definition";
    private static final String FEATURES_PREFIX = "features/";
    private static final String FEATURE_PROPERTY_ON_ANY_FEATURE = "features/*/properties/";
    private static final String FEATURE_PROPERTY_ON_SPECIFIC_FEATURE_TEMPLATE = "features/%s/properties/";
    private static final String FEATURE_DESIRED_PROPERTY_ON_ANY_FEATURE = "features/*/desiredProperties/";
    private static final String FEATURE_DESIRED_PROPERTY_ON_SPECIFIC_FEATURE_TEMPLATE = "features/%s/desiredProperties/";

    private PropertyPaths() {
        throw new AssertionError();
    }

    static String thingId() {
        return THING_ID;
    }

    static String attribute(final String key) {
        requireNonNull(key);

        return ATTRIBUTES_PREFIX + key;
    }

    static String created() {
        return CREATED;
    }

    static String modified() {
        return MODIFIED;
    }

    static String definition() {
        return DEFINITION;
    }

    static String featureProperty(final String key) {
        requireNonNull(key);

        return FEATURE_PROPERTY_ON_ANY_FEATURE + key;
    }

    static String featureProperty(final String featureId, final String key) {
        requireNonNull(featureId);
        requireNonNull(key);

        return String.format(FEATURE_PROPERTY_ON_SPECIFIC_FEATURE_TEMPLATE, featureId) + key;
    }

    static String featureDesiredProperty(final String key) {
        requireNonNull(key);

        return FEATURE_DESIRED_PROPERTY_ON_ANY_FEATURE + key;
    }

    static String featureDesiredProperty(final String featureId, final String key) {
        requireNonNull(featureId);
        requireNonNull(key);

        return String.format(FEATURE_DESIRED_PROPERTY_ON_SPECIFIC_FEATURE_TEMPLATE, featureId) + key;
    }

    static String feature(final String subPath) {
        requireNonNull(subPath);

        return FEATURES_PREFIX + subPath;
    }

}
