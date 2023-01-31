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

import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.thingsearch.model.SearchModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchResult;

/**
 * Extracts the search response from JSON.
 */
public final class SearchResponseExtractors {

    private SearchResponseExtractors() {
        throw new AssertionError();
    }

    public static SearchResult asSearchResult(final String json) {
        return SearchModelFactory.newSearchResult(json);
    }

    public static long asSearchCount(final String json) {
        return JsonFactory.readFrom(json).asLong();
    }

}

