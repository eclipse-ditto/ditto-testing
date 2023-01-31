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
package org.eclipse.ditto.testing.common.things_search;

import javax.annotation.concurrent.Immutable;

/**
 * This class is a builder for search filter expressions.
 */
// Add methods as required.
@Immutable
public final class ThingsSearchFilter {

    private final String searchFilterString;

    private ThingsSearchFilter(final String searchFilterString) {
        this.searchFilterString = searchFilterString;
    }

    public static ThingsSearchFilter empty() {
        return new ThingsSearchFilter("");
    }

    @Override
    public String toString() {
        return searchFilterString;
    }

}
