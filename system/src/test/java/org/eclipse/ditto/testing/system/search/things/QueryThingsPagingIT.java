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
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
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

    private static final String OBJECT_SORT_FIXTURE_KEY = "objectSortFixture";
    private static final String OBJECT_SORT_KEY = "objectSortKey";
    private static final String OBJECT_SORT_FEATURE_ID = "objectSortFeature";
    private static final int MAX_CURSOR_PAGES = 20;

    private static UUID thingIdSuffix;

    @Override
    protected void createTestData() {
        thingIdSuffix = UUID.randomUUID();

        persistThingAndWaitTillAvailable(createSingleThing());
        persistThingsAndWaitTillAvailable(i -> createMultiThing(1, i), 50);

        persistThingAndWaitTillAvailable(createObjectSortThing("m1", null));
        persistThingAndWaitTillAvailable(createObjectSortThing("m2", null));
        persistThingAndWaitTillAvailable(createObjectSortThing("o1", rankObject("a")));
        persistThingAndWaitTillAvailable(createObjectSortThing("o2", rankObject("b")));
        persistThingAndWaitTillAvailable(createObjectSortThing("o3", rankObject("c")));
        persistThingAndWaitTillAvailable(createObjectSortThing("o4", rankObject("d")));
        persistThingAndWaitTillAvailable(createObjectSortThing("o5", rankObject("e")));
    }

    private static JsonObject rankObject(final String rank) {
        return JsonObject.newBuilder().set("rank", rank).build();
    }

    private static Thing createObjectSortThing(final String name, @Nullable final JsonObject sortValue) {
        final Attributes fixtureAttrs = Attributes.newBuilder()
                .set(OBJECT_SORT_FIXTURE_KEY, thingIdSuffix.toString())
                .build();
        if (sortValue == null) {
            return Thing.newBuilder().setId(objectSortThingId(name))
                    .setAttributes(fixtureAttrs)
                    .build();
        }
        final Feature feature = Feature.newBuilder()
                .properties(FeatureProperties.newBuilder().set(OBJECT_SORT_KEY, sortValue).build())
                .withId(OBJECT_SORT_FEATURE_ID)
                .build();
        return Thing.newBuilder().setId(objectSortThingId(name))
                .setAttributes(fixtureAttrs.setValue(OBJECT_SORT_KEY, sortValue))
                .setFeature(feature)
                .build();
    }

    private static ThingId objectSortThingId(final String name) {
        return ThingId.of(idGenerator().withName(QueryThingsPagingIT.class.getSimpleName(), "objsort", name,
                thingIdSuffix.toString()));
    }

    private static List<ThingId> ids(final String... names) {
        final List<ThingId> result = new ArrayList<>(names.length);
        for (final String name : names) {
            result.add(objectSortThingId(name));
        }
        return result;
    }

    private static SearchFilter objectSortFilter() {
        return attribute(OBJECT_SORT_FIXTURE_KEY).eq(thingIdSuffix.toString());
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

    /**
     * Relies on the object values {"rank":"a"}..{"rank":"e"} being assigned in thing-ID order, so that PostgreSQL
     * (objects tie, thing ID decides) and MongoDB (objects by content) share one order; missing keys sort first.
     */
    @Test
    public void pageWithCursorThroughObjectSortValuesAscending() {
        final String sort = "sort(+attributes/" + OBJECT_SORT_KEY + ",+thingId)";
        final List<ThingId> expected = ids("m1", "m2", "o1", "o2", "o3", "o4", "o5");
        assertThat(searchUnpaged(objectSortFilter(), sort)).containsExactlyElementsOf(expected);
        final CursorWalk walk = walkWithCursor(objectSortFilter(), sort, 2);
        assertThat(walk.thingIds).as("walk equals the unpaged order, no thing twice, none missing")
                .containsExactlyElementsOf(expected);
        assertThat(walk.pages).isEqualTo(4);
    }

    /**
     * Relies on the descending thing-ID tiebreak: with {@code -thingId} the tied objects of PostgreSQL come out in
     * the same order as MongoDB's descending object order; missing keys sort last.
     */
    @Test
    public void pageWithCursorThroughObjectSortValuesDescending() {
        final String sort = "sort(-attributes/" + OBJECT_SORT_KEY + ",-thingId)";
        final List<ThingId> expected = ids("o5", "o4", "o3", "o2", "o1", "m2", "m1");
        assertThat(searchUnpaged(objectSortFilter(), sort)).containsExactlyElementsOf(expected);
        final CursorWalk walk = walkWithCursor(objectSortFilter(), sort, 2);
        assertThat(walk.thingIds).as("walk equals the unpaged order, no thing twice, none missing")
                .containsExactlyElementsOf(expected);
        assertThat(walk.pages).isEqualTo(4);
    }

    /**
     * Relies on the feature property holding the same objects as the attribute, aligned with thing-ID order; page
     * size 1 puts every page boundary inside the object group on an object.
     */
    @Test
    public void pageWithCursorThroughObjectFeaturePropertySortValues() {
        final String sort =
                "sort(+features/" + OBJECT_SORT_FEATURE_ID + "/properties/" + OBJECT_SORT_KEY + ",+thingId)";
        final List<ThingId> expected = ids("m1", "m2", "o1", "o2", "o3", "o4", "o5");
        assertThat(searchUnpaged(objectSortFilter(), sort)).containsExactlyElementsOf(expected);
        final CursorWalk walk = walkWithCursor(objectSortFilter(), sort, 1);
        assertThat(walk.thingIds).as("walk equals the unpaged order, no thing twice, none missing")
                .containsExactlyElementsOf(expected);
        assertThat(walk.pages).isEqualTo(7);
    }

    /**
     * The backends order the objects differently under an ascending thing-ID tiebreak, so the walk is compared with
     * this backend's own unpaged result; missing keys sort last descending on both, in thing-ID order.
     */
    @Test
    public void pageWithCursorThroughObjectSortValuesDescendingWithAscendingThingIdTiebreak() {
        final String sort = "sort(-attributes/" + OBJECT_SORT_KEY + ",+thingId)";
        final List<ThingId> unpaged = searchUnpaged(objectSortFilter(), sort);
        assertThat(unpaged).containsExactlyInAnyOrderElementsOf(ids("m1", "m2", "o1", "o2", "o3", "o4", "o5"));
        assertThat(unpaged.subList(5, 7)).containsExactlyElementsOf(ids("m1", "m2"));
        final CursorWalk walk = walkWithCursor(objectSortFilter(), sort, 2);
        assertThat(walk.thingIds).as("walk equals the unpaged order, no thing twice, none missing")
                .containsExactlyElementsOf(unpaged);
        assertThat(walk.pages).isEqualTo(4);
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

    /**
     * Pages with the cursor until Ditto returns none; fails after MAX_CURSOR_PAGES pages (a non-terminating walk).
     */
    private CursorWalk walkWithCursor(final SearchFilter filter, final String sortOption, final int pageSize) {
        final List<ThingId> thingIds = new ArrayList<>();
        int pages = 0;
        String cursor = null;
        do {
            if (pages == MAX_CURSOR_PAGES) {
                throw new AssertionError(
                        "cursor walk did not end within " + MAX_CURSOR_PAGES + " pages; walked so far: " + thingIds);
            }
            final SearchMatcher matcher = search(filter, newSizeOption(pageSize));
            if (cursor == null) {
                matcher.option(sortOption);
            } else {
                matcher.option(newCursorOption(cursor));
            }
            final SearchResult result = SearchModelFactory.newSearchResult(matcher.returnIdsOnly(true)
                    .expectingStatusCodeSuccessful()
                    .fire()
                    .getBody()
                    .asString());
            pages++;
            thingIds.addAll(thingIdsOf(result));
            cursor = result.getCursor().orElse(null);
        } while (cursor != null);
        return new CursorWalk(thingIds, pages);
    }

    private List<ThingId> searchUnpaged(final SearchFilter filter, final String sortOption) {
        final SearchResult result = SearchModelFactory.newSearchResult(
                search(filter, newSizeOption(20)).option(sortOption)
                        .returnIdsOnly(true)
                        .expectingStatusCodeSuccessful()
                        .fire()
                        .getBody()
                        .asString());
        return thingIdsOf(result);
    }

    private static List<ThingId> thingIdsOf(final SearchResult result) {
        final List<ThingId> thingIds = new ArrayList<>();
        for (final JsonValue item : result.getItems()) {
            final JsonValue thingId = item.asObject().getValue("thingId")
                    .orElseThrow(() -> new AssertionError("search result item without thingId: " + item));
            thingIds.add(ThingId.of(thingId.asString()));
        }
        return thingIds;
    }

    private static final class CursorWalk {

        private final List<ThingId> thingIds;
        private final int pages;

        private CursorWalk(final List<ThingId> thingIds, final int pages) {
            this.thingIds = thingIds;
            this.pages = pages;
        }

    }

    private static JsonObject idItem(final ThingId thingId) {
        return JsonFactory.newObject(String.format("{\"thingId\":\"%s\"}", thingId));
    }
}
