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

import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.attribute;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSortOption;

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.testing.common.matcher.search.SortProperties;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.thingsearch.model.SortOption;
import org.eclipse.ditto.thingsearch.model.SortOptionEntry;
import org.junit.Test;

/**
 *
 */
public final class QueryThingsWithSortingBySpecialCharKeyIT extends VersionedSearchIntegrationTest {

    private static final String COMMON_ATTR_KEY = "commonAttr";
    private static final String COMMON_ATTR_VALUE = "commonAttrValue";

    private static final String SPECIAL_CHAR_SORT_ATTR_KEY = "$things.itests.sortingKey";
    private static final String SPECIAL_CHAR_SORT_FEATURE_ID = "$things.itests.myFeature";
    private static final String SPECIAL_CHAR_SORT_FEATURE_PROPERTY_KEY = "things.itests.someKey";
    private static final String SPECIAL_CHAR_SORT_FEATURE_FIELD =
            SPECIAL_CHAR_SORT_FEATURE_ID + "/properties/" + SPECIAL_CHAR_SORT_FEATURE_PROPERTY_KEY;

    private static ThingId thing1Id;
    private static ThingId thing2Id;
    private static ThingId thing3Id;
    private static ThingId thing4Id;

    protected void createTestData() {
        thing1Id = persistThingAndWaitTillAvailable(createThing1());
        thing2Id = persistThingAndWaitTillAvailable(createThing2());
        thing3Id = persistThingAndWaitTillAvailable(createThing3());
        thing4Id = persistThingAndWaitTillAvailable(createThing4());
    }

    private Thing createThing1() {
        return createThing(1, 1, "dttr");
    }

    private Thing createThing2() {
        return createThing(2, 2, "attr");
    }

    private Thing createThing3() {
        return createThing(3, 3, "cttr");
    }

    private Thing createThing4() {
        return createThing(4, 4, "bttr");
    }

    private Thing createThing(final int i, final int sortAttrValue, final String sortPropertyValue) {
        final Attributes attrs = Attributes.newBuilder()
                .set(COMMON_ATTR_KEY, COMMON_ATTR_VALUE)
                .set(SPECIAL_CHAR_SORT_ATTR_KEY, sortAttrValue)
                .build();

        final Feature feature = Feature.newBuilder()
                .properties(FeatureProperties.newBuilder()
                        .set(SPECIAL_CHAR_SORT_FEATURE_PROPERTY_KEY, sortPropertyValue)
                        .build())
                .withId(SPECIAL_CHAR_SORT_FEATURE_ID)
                .build();

        return Thing.newBuilder().setId(createThingId(i))
                .setAttributes(attrs)
                .setFeature(feature)
                .build();
    }

    private ThingId createThingId(final int i) {
        return ThingId.of(idGenerator().withPrefixedRandomName(getClass().getSimpleName(), String.valueOf(i)));
    }

    @Test
    public void sortByAttributeDescending() {
        search(newSortOption(SortProperties.attribute(SPECIAL_CHAR_SORT_ATTR_KEY), SortOptionEntry.SortOrder.DESC),
                apiVersion)
                .expectingBody(isEqualTo(toThingResult(thing4Id, thing3Id, thing2Id, thing1Id)))
                .fire();
    }

    @Test
    public void sortByAttributeAscending() {
        search(newSortOption(SortProperties.attribute(SPECIAL_CHAR_SORT_ATTR_KEY), SortOptionEntry.SortOrder.ASC),
                apiVersion)
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void sortByFeatureAscending() {
        search(newSortOption(SortProperties.feature(SPECIAL_CHAR_SORT_FEATURE_FIELD), SortOptionEntry.SortOrder.ASC),
                apiVersion)
                .expectingBody(isEqualTo(toThingResult(thing2Id, thing4Id, thing3Id, thing1Id)))
                .fire();
    }

    @Test
    public void sortByFeatureDescending() {
        search(newSortOption(SortProperties.feature(SPECIAL_CHAR_SORT_FEATURE_FIELD), SortOptionEntry.SortOrder.DESC),
                apiVersion)
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id, thing2Id)))
                .fire();
    }

    private static SearchMatcher search(final SortOption sortOption, final JsonSchemaVersion apiVersion) {
        return searchThings(apiVersion)
                .filter(attribute(COMMON_ATTR_KEY).eq(COMMON_ATTR_VALUE))
                .option(sortOption);
    }

}
