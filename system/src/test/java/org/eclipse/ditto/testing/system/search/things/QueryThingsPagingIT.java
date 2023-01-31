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
package org.eclipse.ditto.testing.system.search.things;


import static org.eclipse.ditto.testing.common.assertions.IntegrationTestAssertions.assertThat;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.attribute;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.satisfiesSearchResult;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newCursorOption;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newLimitOption;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSizeOption;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.headers.DittoHeaders;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.base.model.signals.GlobalErrorRegistry;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.thingsearch.model.LimitOption;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SearchModelFactory;
import org.eclipse.ditto.thingsearch.model.SearchResult;
import org.eclipse.ditto.thingsearch.model.SizeOption;
import org.eclipse.ditto.thingsearch.model.signals.commands.exceptions.InvalidOptionException;
import org.junit.Test;

/**
 * Test paging options.
 */
public final class QueryThingsPagingIT extends VersionedSearchIntegrationTest {

    private static final int KNOWN_LIMIT = 2;

    private static final String ATTR1_KEY = "key1";
    private static final String ATTR2_KEY = "key2";
    private static final String ATTR3_KEY = "key3";

    private static final String SINGLE_THING_ATTR1_VALUE = "occursOnce";
    private static final String MULTI_THING_ATTR1_VALUE = "occurs50Times";
    private static final String COMMON_ATTR2_VALUE = "same4All";

    private static UUID thingIdSuffix;

    @Override
    protected void createTestData() {
        thingIdSuffix = UUID.randomUUID();

        persistThingAndWaitTillAvailable(createSingleThing());
        persistThingsAndWaitTillAvailable(i -> createMultiThing(1, i), 50);
    }

    private static Thing createSingleThing() {
        final Attributes attrs = Attributes.newBuilder()
                .set(ATTR1_KEY, SINGLE_THING_ATTR1_VALUE)
                .set(ATTR2_KEY, COMMON_ATTR2_VALUE)
                .set(ATTR3_KEY, randomString())
                .build();

        return Thing.newBuilder().setId(createThingId(0))
                .setAttributes(attrs)
                .build();
    }

    private static Thing createMultiThing(final int startIndex, final long i) {
        final Attributes attrs = Attributes.newBuilder()
                .set(ATTR1_KEY, MULTI_THING_ATTR1_VALUE)
                .set(ATTR2_KEY, COMMON_ATTR2_VALUE)
                .set(ATTR3_KEY, randomString())
                .build();

        return Thing.newBuilder().setId(createThingId(startIndex + i))
                .setAttributes(attrs)
                .build();
    }

    private static ThingId createThingId(final long i) {
        final String formattedIndex = formatIndex(i);
        return ThingId.of(idGenerator().withName(QueryThingsPagingIT.class.getSimpleName(), formattedIndex,
                thingIdSuffix.toString()));
    }

    private static String formatIndex(final long i) {
        return String.format("%03d", i);
    }

    /*
     * Tests for cursor-based paging
     */
    @Test
    public void pageItemsWithCursor() {
        final String nonexistentAttr1 = "attributes/nonexistent/1";
        final String nonexistentAttr2 = "attributes/nonexistent/2";
        final SearchFilter filter = attribute(ATTR2_KEY).eq(COMMON_ATTR2_VALUE);
        final String sortOption =
                String.format("sort(-%s,+attributes/%s,+%s)", nonexistentAttr1, ATTR2_KEY, nonexistentAttr2);

        final Supplier<AssertionError> expectNonemptyCursor = () -> new AssertionError("Expect nonempty cursor");

        // page with items count less than limit
        final SearchResult result0 = SearchModelFactory.newSearchResult(
                search(filter, newSizeOption(1)).option(sortOption)
                        .returnIdsOnly(true)
                        .expectingStatusCodeSuccessful()
                        .fire()
                        .getBody()
                        .asString());

        final String cursor0 = result0.getCursor().orElseThrow(expectNonemptyCursor);
        assertThat(result0.getItems()).hasSize(1);
        assertThat(result0.getItems()).contains(idItem(createThingId(0)));

        // page with items count equal to limit
        final int totalCount = 51;
        final SearchResult result1 = SearchModelFactory.newSearchResult(
                search(filter, newSizeOption(totalCount)).returnIdsOnly(true)
                        .expectingStatusCodeSuccessful()
                        .fire()
                        .getBody()
                        .asString());
        assertThat(result1.getItems().getSize()).isEqualTo(totalCount);
        assertThat(result1.hasNextPage()).isFalse();

        // page with skip and limit less than total items
        final SearchResult result2 = SearchModelFactory.newSearchResult(
                search(filter, newSizeOption(1)).option(newCursorOption(cursor0))
                        .returnIdsOnly(true)
                        .expectingStatusCodeSuccessful()
                        .fire()
                        .getBody()
                        .asString());

        final String cursor2 = result2.getCursor().orElseThrow(expectNonemptyCursor);
        assertThat(result2.getItems()).hasSize(1);
        assertThat(result2.getItems()).contains(idItem(createThingId(1)));

        // page with items count equal to limit
        final int remainingCount = totalCount - 2;
        final SearchResult result3 = SearchModelFactory.newSearchResult(
                search(filter, newSizeOption(remainingCount)).option(newCursorOption(cursor2))
                        .returnIdsOnly(true)
                        .expectingStatusCodeSuccessful()
                        .fire()
                        .getBody()
                        .asString());
        assertThat(result3.hasNextPage()).isFalse();
        assertThat(result3.getItems()).hasSize(remainingCount);
        assertThat(result3.getItems()).contains(idItem(createThingId(50)));
        assertThat(result3.getItems()).doesNotContain(idItem(createThingId(1)));
    }

    /*
     * Tests for the deprecated "limit" option
     */

    @Test
    public void pageWithItemsCountLessThanLimit() {
        // prepare
        final List<ThingId> oneThingList = Collections.singletonList(createThingId(0));
        final SearchFilter filter = attribute(ATTR1_KEY).eq(SINGLE_THING_ATTR1_VALUE);
        final SearchResult expectedList = toThingResult(SearchResult.NO_NEXT_PAGE, oneThingList);

        // test
        searchWithLimit(filter, newLimitOption(0, KNOWN_LIMIT))
                .expectingBody(isEqualTo(expectedList))
                .fire();
    }

    @Test
    public void pageWithItemsCountEqualToLimit() {
        // prepare
        final SearchFilter filter = attribute(ATTR2_KEY).eq(COMMON_ATTR2_VALUE);
        final SearchResult expectedList = toThingResult(KNOWN_LIMIT, createThingId(0), createThingId(1));

        // test
        searchWithLimit(filter, newLimitOption(0, KNOWN_LIMIT))
                .expectingBody(isEqualTo(expectedList))
                .fire();
    }

    @Test
    public void pageWithSkipAndLimitLessThanTotalItems() {
        // prepare
        final SearchFilter filter = attribute(ATTR2_KEY).eq(COMMON_ATTR2_VALUE);
        final SearchResult expectedList =
                toThingResult(KNOWN_LIMIT + KNOWN_LIMIT, createThingId(2), createThingId(3));

        // test
        searchWithLimit(filter, newLimitOption(KNOWN_LIMIT, KNOWN_LIMIT))
                .expectingBody(isEqualTo(expectedList))
                .fire();
    }

    @Test
    public void lastPageWithItemsCountLessThanLimit() {
        // prepare
        final SearchFilter filter = attribute(ATTR2_KEY).eq(COMMON_ATTR2_VALUE);
        final SearchResult expectedList = toThingResult(SearchResult.NO_NEXT_PAGE, createThingId(50));

        // test
        searchWithLimit(filter, newLimitOption(50, KNOWN_LIMIT))
                .expectingBody(isEqualTo(expectedList))
                .fire();
    }

    @Test
    public void lastPageWithItemsCountEqualToLimit() {
        // prepare
        final SearchFilter filter = attribute(ATTR1_KEY).ne(SINGLE_THING_ATTR1_VALUE);
        final SearchResult expectedList =
                toThingResult(SearchResult.NO_NEXT_PAGE, createThingId(49), createThingId(50));

        // test
        searchWithLimit(filter, newLimitOption(48, KNOWN_LIMIT))
                .expectingBody(isEqualTo(expectedList))
                .fire();
    }

    @Test
    public void defaultLimitValue() {
        // prepare
        final SearchFilter filter = attribute(ATTR2_KEY).eq(COMMON_ATTR2_VALUE);
        final Collection<ThingId> expectedIdsList = new ArrayList<>();
        for (int i = 0; i < DEFAULT_PAGE_SIZE; i++) {
            expectedIdsList.add(createThingId(i));
        }
        final SearchResult expectedList = toThingResult(DEFAULT_PAGE_SIZE, expectedIdsList);

        // test
        searchWithLimit(filter, null)
                .expectingBody(satisfiesSearchResult(result -> {
                    assertThat(result.getItems()).isEqualTo(expectedList.getItems());
                    assertThat(result.hasNextPage()).isEqualTo(expectedList.hasNextPage());
                }))
                .fire();
    }

    @Test
    public void limitValueExceedsMaximum() {
        // prepare
        final SearchFilter filter = attribute(ATTR2_KEY).eq(COMMON_ATTR2_VALUE);
        final int limitValueExceedingMaximum = MAX_PAGE_SIZE + 1;

        // test
        searchWithLimit(filter, newLimitOption(0, limitValueExceedingMaximum))
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(satisfies(
                        body -> {
                            final String errorCode = GlobalErrorRegistry.getInstance().parse(body,
                                    DittoHeaders.empty()).getErrorCode();
                            assertThat(errorCode).isEqualTo(InvalidOptionException.ERROR_CODE);
                        })
                )
                .fire();
    }

    private SearchMatcher searchWithLimit(final SearchFilter searchFilter, @Nullable final LimitOption limitOption) {
        return search(searchFilter, null, limitOption, apiVersion);
    }

    private SearchMatcher search(final SearchFilter searchFilter, @Nullable final SizeOption sizeOption) {
        return search(searchFilter, sizeOption, null, apiVersion);
    }

    private static SearchMatcher search(final SearchFilter searchFilter,
            @Nullable final SizeOption sizeOption,
            @Nullable final LimitOption limitOption, final JsonSchemaVersion apiVersion) {

        final SearchMatcher matcher = searchThings(apiVersion).filter(searchFilter);

        if (limitOption != null) {
            matcher.option(limitOption);
        }
        if (sizeOption != null) {
            matcher.option(sizeOption);
        }

        return matcher;
    }

    private static JsonObject idItem(final ThingId thingId) {
        return JsonFactory.newObject(String.format("{\"thingId\":\"%s\"}", thingId));
    }
}
