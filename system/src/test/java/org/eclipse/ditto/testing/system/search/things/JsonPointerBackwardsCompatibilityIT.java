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


import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isCount;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSortOption;

import javax.annotation.Nullable;

import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SortProperties;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.thingsearch.model.SortOption;
import org.eclipse.ditto.thingsearch.model.SortOptionEntry;
import org.junit.Test;

/**
 * Checks that Json Pointers used in sort or filter expressions do not need a leading slash.
 */
public final class JsonPointerBackwardsCompatibilityIT extends VersionedSearchIntegrationTest {

    private static final String ATTR_KEY = "attrKey";
    private static final String ATTR_VALUE_1 = "a";
    private static final String ATTR_VALUE_2 = "b";
    private static final SortOption SORT_BY_THING_ID =
            newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC);

    private static final String FILTER_ATTR_VALUE_1 = "eq(attributes/" + ATTR_KEY + ",\"" + ATTR_VALUE_1 + "\")";
    private static final String SORT_DESC_BY_ATTR_KEY = "sort(-attributes/" + ATTR_KEY + ")";

    private static ThingId thingId1;
    private static ThingId thingId2;

    protected void createTestData() {
        thingId1 = persistThingAndWaitTillAvailable(createThing1());
        thingId2 = persistThingAndWaitTillAvailable(createThing2());
    }

    private Thing createThing1() {
        return createThing(1, ATTR_VALUE_1);
    }

    private Thing createThing2() {
        return createThing(2, ATTR_VALUE_2);
    }

    private Thing createThing(final int counter, final String attrValue) {
        final Attributes attrs = Attributes.newBuilder()
                .set(ATTR_KEY, attrValue)
                .build();
        return Thing.newBuilder()
                .setId(createThingId(counter))
                .setAttributes(attrs)
                .build();
    }

    private ThingId createThingId(final int counter) {
        return ThingId.of(idGenerator().withPrefixedRandomName(getClass().getSimpleName(), String.valueOf(counter)));
    }

    @Test
    public void filterQuery() {
        searchThings(apiVersion)
                .filter(createFilter(FILTER_ATTR_VALUE_1))
                .option(SORT_BY_THING_ID)
                .expectingBody(isEqualTo(toThingResult(thingId1)))
                .fire();
    }

    @Test
    public void sortQuery() {
        searchThings(apiVersion)
                .filter(createFilter(null))
                .option(SORT_DESC_BY_ATTR_KEY)
                .expectingBody(isEqualTo(toThingResult(thingId2, thingId1)))
                .fire();
    }

    @Test
    public void filterCount() {
        searchCount(apiVersion)
                .filter(createFilter(FILTER_ATTR_VALUE_1))
                .expectingBody(isCount(1))
                .fire();
    }

    private String createFilter(@Nullable final String filter) {
        final var inThingIdFilter = String.format("in(thingId,\"%s\",\"%s\")", thingId1, thingId2);
        if (filter == null) {
            return inThingIdFilter;
        } else {
            return String.format("and(%s,%s)", filter, inThingIdFilter);
        }
    }

}
