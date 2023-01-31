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
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSortOptionEntry;

import java.util.Arrays;
import java.util.List;

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.testing.common.ServiceEnvironment;
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

public final class QueryThingsWithSortingIT extends VersionedSearchIntegrationTest {

    private static final String COMMON_ATTR_KEY = "commonAttr";
    private static final String COMMON_ATTR_VALUE = "commonAttrValue";

    private static final String SORT_ATTR_NUMBER_KEY = "sortAttrNumber";
    private static final String SORT_ATTR_STRING_KEY = "sortAttrString";
    private static final String SORT_FEATURE_ID = "sortFeature";
    private static final String SORT_FEATURE_PROP_PARENT_KEY = "status";
    private static final String SORT_FEATURE_PROP_STRING_KEY = "propString";
    private static final String SORT_FEATURE_FIELD_STRING = SORT_FEATURE_ID + "/properties/" +
            SORT_FEATURE_PROP_PARENT_KEY + "/" + SORT_FEATURE_PROP_STRING_KEY;
    private static final String SORT_FEATURE_PROP_BOOL_KEY = "propBool";
    private static final String SORT_FEATURE_FIELD_BOOL = SORT_FEATURE_ID + "/properties/" +
            SORT_FEATURE_PROP_PARENT_KEY + "/" + SORT_FEATURE_PROP_BOOL_KEY;
    private static final String RANDOM_NAMESPACE = ServiceEnvironment.createRandomDefaultNamespace();

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
        return createThing(1, "elvis", 1, "dttr", false);
    }

    private Thing createThing2() {
        return createThing(2, "bodo", 2, "attr", true);
    }

    private Thing createThing3() {
        return createThing(3, "Béla Réthy",3, "cttr", false);
    }

    private Thing createThing4() {
        return createThing(4, "zidane", 4, "bttr", true);
    }

    private Thing createThing(final int i, final String attrValueString, final int attrValueNumber,
            final String propValueString, final boolean propValueBoolean) {
        final Attributes attrs = Attributes.newBuilder()
                .set(COMMON_ATTR_KEY, COMMON_ATTR_VALUE)
                .set(SORT_ATTR_STRING_KEY, attrValueString)
                .set(SORT_ATTR_NUMBER_KEY, attrValueNumber)
                .build();

        final JsonObject complexPropValue = JsonObject.newBuilder()
                .set(SORT_FEATURE_PROP_STRING_KEY, propValueString)
                .set(SORT_FEATURE_PROP_BOOL_KEY, propValueBoolean)
                .build();
        final Feature feature = Feature.newBuilder()
                .properties(FeatureProperties.newBuilder()
                        .set(SORT_FEATURE_PROP_PARENT_KEY, complexPropValue)
                        .build()
                )
                .withId(SORT_FEATURE_ID)
                .build();

        return Thing.newBuilder().setId(createThingId(i))
                .setAttributes(attrs)
                .setFeature(feature)
                .build();
    }

    private ThingId createThingId(final int i) {
        return ThingId.of(idGenerator(RANDOM_NAMESPACE)
                .withPrefixedRandomName(getClass().getSimpleName(), String.valueOf(i)));
    }

    @Test
    public void sortByThingIdAscending() {
        search(newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void sortByThingIdDescending() {
        search(newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.DESC))
                .expectingBody(isEqualTo(toThingResult(thing4Id, thing3Id, thing2Id, thing1Id)))
                .fire();
    }

    @Test
    public void sortBySortStringAttrAscendingAndThingIdAscending() {
        final List<SortOptionEntry> sortOptionEntries = Arrays.asList(
                newSortOptionEntry(SortProperties.attribute(SORT_ATTR_STRING_KEY), SortOptionEntry.SortOrder.ASC),
                newSortOptionEntry(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC)
        );
        final SortOption sortOption = newSortOption(sortOptionEntries);

        search(sortOption)
                .expectingBody(isEqualTo(toThingResult(thing3Id, thing2Id, thing1Id, thing4Id)))
                .fire();
    }

    @Test
    public void sortBySortStringAttrDescendingAndThingIdDescending() {
        final List<SortOptionEntry> sortOptionEntries = Arrays.asList(
                newSortOptionEntry(SortProperties.attribute(SORT_ATTR_STRING_KEY), SortOptionEntry.SortOrder.DESC),
                newSortOptionEntry(SortProperties.thingId(), SortOptionEntry.SortOrder.DESC)
        );
        final SortOption sortOption = newSortOption(sortOptionEntries);

        search(sortOption)
                .expectingBody(isEqualTo(toThingResult(thing4Id, thing1Id, thing2Id, thing3Id)))
                .fire();
    }

    @Test
    public void sortByAttributeDescending() {
        search(newSortOption(SortProperties.attribute(SORT_ATTR_NUMBER_KEY), SortOptionEntry.SortOrder.DESC))
                .expectingBody(isEqualTo(toThingResult(thing4Id, thing3Id, thing2Id, thing1Id)))
                .fire();
    }

    @Test
    public void sortByAttributeAscending() {
        search(newSortOption(SortProperties.attribute(SORT_ATTR_NUMBER_KEY), SortOptionEntry.SortOrder.ASC))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void sortByFeatureAscending() {
        search(newSortOption(SortProperties.feature(SORT_FEATURE_FIELD_STRING), SortOptionEntry.SortOrder.ASC))
                .expectingBody(isEqualTo(toThingResult(thing2Id, thing4Id, thing3Id, thing1Id)))
                .fire();
    }

    @Test
    public void sortByFeatureDescending() {
        search(newSortOption(SortProperties.feature(SORT_FEATURE_FIELD_STRING), SortOptionEntry.SortOrder.DESC))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id, thing2Id)))
                .fire();
    }

    @Test
    public void sortByStringFeatureFieldAscendingAndByBoolFeatureFieldDescending() {
        final List<SortOptionEntry> sortOptionEntries = Arrays.asList(
                newSortOptionEntry(SortProperties.feature(SORT_FEATURE_FIELD_BOOL), SortOptionEntry.SortOrder.ASC),
                newSortOptionEntry(SortProperties.feature(SORT_FEATURE_FIELD_STRING), SortOptionEntry.SortOrder.DESC)
        );
        final SortOption sortOption = newSortOption(sortOptionEntries);

        search(sortOption)
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id, thing2Id)))
                .fire();
    }

    @Test
    public void sortByFeatureFieldDescendingAndByAttributeFieldAscending() {
        final List<SortOptionEntry> sortOptionEntries = Arrays.asList(
                newSortOptionEntry(SortProperties.feature(SORT_FEATURE_FIELD_BOOL), SortOptionEntry.SortOrder.DESC),
                newSortOptionEntry(SortProperties.attribute(SORT_ATTR_NUMBER_KEY), SortOptionEntry.SortOrder.ASC)
        );
        final SortOption sortOption = newSortOption(sortOptionEntries);

        search(sortOption)
                .expectingBody(isEqualTo(toThingResult(thing2Id, thing4Id, thing1Id, thing3Id)))
                .fire();
    }

    private SearchMatcher search(final SortOption sortOption) {
        return search(sortOption, apiVersion);
    }

    private static SearchMatcher search(final SortOption sortOption, final JsonSchemaVersion apiVersion) {
        return searchThings(apiVersion)
                .namespaces(RANDOM_NAMESPACE)
                .filter(attribute(COMMON_ATTR_KEY).eq(COMMON_ATTR_VALUE))
                .option(sortOption);
    }

}
