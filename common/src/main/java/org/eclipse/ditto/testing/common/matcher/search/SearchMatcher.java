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
import static org.eclipse.ditto.base.model.common.ConditionChecker.checkNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.testing.common.CommonTestConfig;
import org.eclipse.ditto.testing.common.HttpResource;
import org.eclipse.ditto.testing.common.matcher.AbstractGetMatcher;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.thingsearch.model.CursorOption;
import org.eclipse.ditto.thingsearch.model.LimitOption;
import org.eclipse.ditto.thingsearch.model.Option;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SizeOption;
import org.eclipse.ditto.thingsearch.model.SortOption;
import org.slf4j.Logger;

/**
 * This matcher requests a GET for a resource of Ditto Search and checks if it worked as expected.
 */
@NotThreadSafe
public final class SearchMatcher extends AbstractGetMatcher<SearchMatcher> {

    /**
     * Parameter filter.
     */
    public static final String PARAM_FILTER = "filter";

    /**
     * Parameter option.
     */
    public static final String PARAM_OPTION = "option";

    private static final String PARAM_NAMESPACE = "namespaces";

    private static final String GET_MESSAGE_TEMPLATE =
            "GETting search results from <{}> with filter <{}> and options <{}>.";

    private SearchMatcher(final String path) {
        super(path);
        expectingHttpStatus(HttpStatus.OK);
    }

    /**
     * Creates a new {@code SearchMatcher} for the given {@code path}.
     *
     * @param version the version of the resource to get.
     */
    public static SearchMatcher getResult(final JsonSchemaVersion version) {
        requireNonNull(version);
        return new SearchMatcher(buildSearchApiUrl(version));
    }

    /**
     * Creates a new {@code SearchMatcher} for the given {@code path}.
     *
     * @param version the version of the resource to get.
     */
    public static SearchMatcher getCount(final JsonSchemaVersion version) {
        requireNonNull(version);
        return new SearchMatcher(buildSearchApiUrl(version) + HttpResource.COUNT);
    }

    public SearchMatcher filter(final SearchFilter searchFilter) {
        checkNotNull(searchFilter, "searchFilter");
        parameters.put(PARAM_FILTER, Collections.singletonList(searchFilter.toString()));
        return this;
    }

    public SearchMatcher filter(final String searchFilter) {
        checkNotNull(searchFilter, "searchFilter");
        parameters.put(PARAM_FILTER, Collections.singletonList(searchFilter));
        return this;
    }

    public SearchMatcher option(final SortOption sortOption) {
        checkNotNull(sortOption, "sort option");
        addOptionParamValues(Collections.singletonList(sortOption));
        return this;
    }

    public SearchMatcher option(final LimitOption limitOption) {
        checkNotNull(limitOption, "limit option");
        addOptionParamValues(Collections.singletonList(limitOption));
        return this;
    }

    public SearchMatcher option(final SizeOption sizeOption) {
        checkNotNull(sizeOption, "size option");
        addOptionParamValues(Collections.singletonList(sizeOption));
        return this;
    }

    public SearchMatcher option(final CursorOption cursorOption) {
        checkNotNull(cursorOption, "cursor option");
        addOptionParamValues(Collections.singletonList(cursorOption));
        return this;
    }

    public SearchMatcher options(final SortOption sortOption, final SizeOption sizeOption) {
        checkNotNull(sortOption, "sort option");
        checkNotNull(sizeOption, "size option");
        addOptionParamValues(Arrays.asList(sortOption, sizeOption));
        return this;
    }

    public SearchMatcher option(final String options) {
        checkNotNull(options, "options");
        addOption(options);
        return this;
    }

    public SearchMatcher removeOptions() {
        parameters.remove(PARAM_OPTION);
        return this;
    }

    private void addOptionParamValues(final List<Option> options) {
        final String optionValue = options.stream()
                .map(Option::toString)
                .collect(Collectors.joining(","));

        addOption(optionValue);
    }

    private void addOption(final String optionValue) {
        parameters.compute(PARAM_OPTION, (key, value) -> {
            if (value == null) {
                return Collections.singletonList(optionValue);
            } else {
                final List<String> newValue = new ArrayList<>(value);
                newValue.set(0, value.get(0) + "," + optionValue);
                return newValue;
            }
        });
    }

    public SearchMatcher returnIdsOnly(final boolean idsOnly) {
        if (idsOnly) {
            return withFields(JsonFactory.newFieldSelector(Thing.JsonFields.ID));
        } else {
            return withFields(null);
        }
    }

    @Override
    protected void doLog(final Logger logger, final String path, final String entityType) {
        logger.info(GET_MESSAGE_TEMPLATE, path, parameters.get(PARAM_FILTER), parameters.get(PARAM_OPTION));
    }

    public SearchMatcher namespaces(final String... namespaces) {
        checkNotNull(namespaces);
        parameters.put(PARAM_NAMESPACE, Collections.singletonList(String.join(",", namespaces)));
        return this;
    }

    private static String buildSearchApiUrl(final JsonSchemaVersion version) {
        return CommonTestConfig.getInstance().getGatewayApiUrl(version.toInt(), HttpResource.SEARCH.getPath());
    }
}
