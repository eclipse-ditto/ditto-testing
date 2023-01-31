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
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.util.function.Consumer;

import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.matcher.SatisfiesMatcher;
import org.eclipse.ditto.thingsearch.model.SearchResult;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;

/**
 * Matchers for verifying search results.
 */
public final class SearchResponseMatchers {

    private SearchResponseMatchers() {
        throw new AssertionError();
    }

    public static Matcher<String> isEmpty() {
        return satisfiesSearchResult(result -> {
            assertThat(result.getItems()).isEmpty();
            assertThat(result.hasNextPage()).isFalse();
        });
    }

    public static Matcher<String> isEqualTo(final SearchResult expected) {
        requireNonNull(expected);

        return createSearchResultMatcher(equalTo(expected));
    }

    public static Matcher<String> satisfiesSearchResult(final Consumer<SearchResult> predicate) {
        requireNonNull(predicate);

        return createSearchResultMatcher(new SatisfiesMatcher<>(predicate));
    }

    public static Matcher<String> isSingleResult(final Consumer<JsonObject> consumer) {
        return createSearchResultMatcher(new SatisfiesMatcher<>(actualResult -> {
            final JsonObject actualJsonObject = extractSingleResult(actualResult);
            consumer.accept(actualJsonObject);
        }));
    }

    public static Matcher<String> isSingleResultEqualTo(final JsonObject expectedJsonObject) {
        return isSingleResult(
                actualJsonObject -> {
                    try {
                        JSONAssert.assertEquals(expectedJsonObject.toString(), actualJsonObject.toString(), false);
                    } catch (final JSONException e) {
                        throw new AssertionError(e);
                    }
                });
    }

    private static JsonObject extractSingleResult(final SearchResult searchResult) {
        assertThat(searchResult.getItems()).hasSize(1);
        return searchResult.getItems().get(0)
                .map(JsonValue::asObject)
                .orElseThrow(IllegalStateException::new);
    }

    public static Matcher<String> isCountGte(final long min) {
        return createSearchCountMatcher(greaterThanOrEqualTo(min));
    }

    public static Matcher<String> isCount(final long expectedCount) {
        return createSearchCountMatcher(equalTo(expectedCount));
    }

    private static Matcher<String> createSearchCountMatcher(final Matcher<Long> subMatcher) {
        return new FeatureMatcher<String, Long>(subMatcher, "count", "count") {

            @Override
            protected Long featureValueOf(final String actual) {
                return SearchResponseExtractors.asSearchCount(actual);
            }
        };
    }

    private static Matcher<String> createSearchResultMatcher(final Matcher<SearchResult> subMatcher) {
        return new FeatureMatcher<String, SearchResult>(subMatcher, "result", "result") {

            @Override
            protected SearchResult featureValueOf(final String actual) {
                return SearchResponseExtractors.asSearchResult(actual);
            }
        };
    }

}

