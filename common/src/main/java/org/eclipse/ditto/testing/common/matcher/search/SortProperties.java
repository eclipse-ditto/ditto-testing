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
 * Contains factory methods to create property paths for sorting.
 */
public final class SortProperties {

    private static final String THING_ID = PropertyPaths.thingId();

    private SortProperties() {
        throw new AssertionError();
    }

    public static String thingId() {
        return THING_ID;
    }

    public static String attribute(final String key) {
        requireNonNull(key);

        return PropertyPaths.attribute(key);
    }

    public static String featureProperty(final String featureId, final String key) {
        requireNonNull(featureId);
        requireNonNull(key);

        return PropertyPaths.featureProperty(featureId, key);
    }

    public static String feature(final String subPath) {
        requireNonNull(subPath);

        return PropertyPaths.feature(subPath);
    }

}
