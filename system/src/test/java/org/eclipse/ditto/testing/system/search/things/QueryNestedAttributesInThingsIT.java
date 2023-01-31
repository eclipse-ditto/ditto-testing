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


import static java.util.Objects.requireNonNull;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.attribute;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSortOption;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSortOptionEntry;
import static org.hamcrest.Matchers.containsString;

import java.util.Arrays;
import java.util.List;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.testing.common.matcher.search.SortProperties;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SortOption;
import org.eclipse.ditto.thingsearch.model.SortOptionEntry;
import org.junit.Test;

/**
 *
 */
public final class QueryNestedAttributesInThingsIT extends VersionedSearchIntegrationTest {

    private static final String ATTR_KEY_DOES_LIKE = "does_like";
    private static final String ATTR_KEY_FAVORITE_DISH = "favorite_dish";
    private static final String ATTR_VALUE_DOENER = "Doener";
    private static final String ATTR_KEY_FAVORITE_CAR = "favorite_car";
    private static final String ATTR_KEY_AUDI = "Audi";
    private static final String ATTR_VALUE_Q7 = "Q7";
    private static final String ATTR_KEY_LOVES_BUGATTI = "loves_Bugatti";
    private static final String ATTR_KEY_LIVES_IN = "lives_in";
    private static final String ATTR_KEY_STREET = "street";
    private static final String ATTR_KEY_HOUSE_NUMBER = "house_number";
    private static final int ATTR_VALUE_12 = 12;
    private static final String ATTR_KEY_POST_CODE = "post_code";

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
        final Attributes attrs = Attributes.newBuilder()
                .set(ATTR_KEY_DOES_LIKE, JsonObject.newBuilder()
                        .set(ATTR_KEY_FAVORITE_DISH, ATTR_VALUE_DOENER)
                        .set(ATTR_KEY_FAVORITE_CAR, JsonObject.newBuilder()
                                .set(ATTR_KEY_AUDI, ATTR_VALUE_Q7)
                                .set(ATTR_KEY_LOVES_BUGATTI, true)
                                .build())
                        .build())
                .set(ATTR_KEY_LIVES_IN, JsonObject.newBuilder()
                        .set(ATTR_KEY_STREET, "schussenstrasse")
                        .set(ATTR_KEY_HOUSE_NUMBER, ATTR_VALUE_12)
                        .set(ATTR_KEY_POST_CODE, 11223)
                        .build())
                .build();
        return Thing.newBuilder().setId(createThingId(1))
                .setAttributes(attrs)
                .build();
    }

    private Thing createThing2() {
        final Attributes attrs = Attributes.newBuilder()
                .set(ATTR_KEY_DOES_LIKE, JsonObject.newBuilder()
                        .set(ATTR_KEY_FAVORITE_DISH, ATTR_VALUE_DOENER)
                        .set(ATTR_KEY_FAVORITE_CAR, JsonObject.newBuilder()
                                .set("Renault", "Twingo")
                                .set(ATTR_KEY_LOVES_BUGATTI, false)
                                .build())
                        .build())
                .set(ATTR_KEY_LIVES_IN, JsonObject.newBuilder()
                        .set(ATTR_KEY_STREET, "Donald-Duck-Strasse")
                        .set(ATTR_KEY_HOUSE_NUMBER, 17)
                        .set(ATTR_KEY_POST_CODE, 85658)
                        .build())
                .build();
        return Thing.newBuilder().setId(createThingId(2))
                .setAttributes(attrs)
                .build();
    }

    private Thing createThing3() {
        final Attributes attrs = Attributes.newBuilder()
                .set(ATTR_KEY_DOES_LIKE, JsonObject.newBuilder()
                        .set(ATTR_KEY_FAVORITE_DISH, ATTR_VALUE_DOENER)
                        .set(ATTR_KEY_FAVORITE_CAR, JsonObject.newBuilder()
                                .set(ATTR_KEY_AUDI, "R8")
                                .set(ATTR_KEY_LOVES_BUGATTI, false)
                                .build())
                        .build())
                .set(ATTR_KEY_LIVES_IN, JsonObject.newBuilder()
                        .set(ATTR_KEY_STREET, "langenstrasse")
                        .set(ATTR_KEY_HOUSE_NUMBER, ATTR_VALUE_12)
                        .set(ATTR_KEY_POST_CODE, 54545)
                        .build())
                .build();
        return Thing.newBuilder().setId(createThingId(3))
                .setAttributes(attrs)
                .build();
    }

    private Thing createThing4() {
        final Attributes attrs = Attributes.newBuilder()
                .set(ATTR_KEY_DOES_LIKE, JsonObject.newBuilder()
                        .set(ATTR_KEY_FAVORITE_DISH, ATTR_VALUE_DOENER)
                        .set(ATTR_KEY_FAVORITE_CAR, JsonObject.newBuilder()
                                .set(ATTR_KEY_AUDI, "TT")
                                .set(ATTR_KEY_LOVES_BUGATTI, true)
                                .build())
                        .build())
                .set(ATTR_KEY_LIVES_IN, JsonObject.newBuilder()
                        .set(ATTR_KEY_STREET, "rosenweg")
                        .set(ATTR_KEY_HOUSE_NUMBER, 6)
                        .set(ATTR_KEY_POST_CODE, JsonFactory.nullLiteral())
                        .build())
                .build();
        return Thing.newBuilder().setId(createThingId(4))
                .setAttributes(attrs)
                .build();
    }

    private ThingId createThingId(final int i) {
        return ThingId.of(idGenerator().withPrefixedRandomName(getClass().getSimpleName(), String.valueOf(i)));
    }

    @Test
    public void queryOneThing() {
        search(attribute(attrPath(ATTR_KEY_DOES_LIKE, ATTR_KEY_FAVORITE_CAR, ATTR_KEY_AUDI)).eq(ATTR_VALUE_Q7))
                .expectingBody(isEqualTo(toThingResult(thing1Id)))
                .fire();
    }

    @Test
    public void queryTwoThings() {
        search(attribute(attrPath(ATTR_KEY_DOES_LIKE, ATTR_KEY_FAVORITE_CAR, ATTR_KEY_LOVES_BUGATTI)).eq(true))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryTwoThingsWithIntAttribute() {
        search(attribute(attrPath(ATTR_KEY_LIVES_IN, ATTR_KEY_HOUSE_NUMBER)).eq(ATTR_VALUE_12))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id)))
                .fire();
    }

    @Test
    public void queryNullAttribute() {
        search(attribute(attrPath(ATTR_KEY_LIVES_IN, ATTR_KEY_POST_CODE)).eq(null))
                .expectingBody(isEqualTo(toThingResult(thing4Id)))
                .fire();
    }

    // because of the "{" which is not allowed in our parser
    @Test
    public void queryForObjectAsNode() {
        final String filterStr = "eq(attributes/" + attrPath(ATTR_KEY_DOES_LIKE, ATTR_KEY_FAVORITE_CAR) + "," +
                "{\\\"Audi\\\":\\\"Q7\\\"})";
        search(filterStr)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(containsString("Invalid input '{'"))
                .fire();
    }

    @Test
    public void queryThingWithLikePredicate() {
        search(attribute(attrPath(ATTR_KEY_LIVES_IN, ATTR_KEY_STREET)).like("schuss*"))
                .expectingBody(isEqualTo(toThingResult(thing1Id)))
                .fire();
    }

    @Test
    public void queryTwoThingsWithLikePredicate() {
        search(attribute(attrPath(ATTR_KEY_DOES_LIKE, ATTR_KEY_FAVORITE_DISH)).like(ATTR_VALUE_DOENER))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();
    }

    // sort by one criteria
    @Test
    public void sortWithSingleAttributeDescending() {
        search(newSortOption(SortProperties.attribute(attrPath(ATTR_KEY_LIVES_IN, ATTR_KEY_POST_CODE)), SortOptionEntry.SortOrder.DESC
        ))
                .expectingBody(isEqualTo(toThingResult(thing2Id, thing3Id, thing1Id, thing4Id)))
                .fire();
    }

    // sort by two criteria
    @Test
    public void sortByTwoAttributesAscending() {
        final List<SortOptionEntry> sortOptionEntries = Arrays.asList(
                newSortOptionEntry(SortProperties.attribute(attrPath(ATTR_KEY_DOES_LIKE, ATTR_KEY_FAVORITE_CAR,
                        ATTR_KEY_LOVES_BUGATTI)), SortOptionEntry.SortOrder.DESC),
                newSortOptionEntry(SortProperties.attribute(attrPath(ATTR_KEY_LIVES_IN, ATTR_KEY_POST_CODE)),
                        SortOptionEntry.SortOrder.ASC)
        );
        final SortOption sortOption = newSortOption(sortOptionEntries);

        search(sortOption)
                .expectingBody(isEqualTo(toThingResult(thing4Id, thing1Id, thing3Id, thing2Id)))
                .fire();
    }

    private static String attrPath(final String... keys) {
        requireNonNull(keys);

        return String.join("/", keys);
    }

    private SearchMatcher search(final SortOption sortOption) {
        return search(sortOption, apiVersion);
    }

    private static SearchMatcher search(final SortOption sortOption, final JsonSchemaVersion apiVersion) {
        return searchThings(apiVersion)
                .filter(attribute(attrPath(ATTR_KEY_DOES_LIKE, ATTR_KEY_FAVORITE_DISH)).eq(ATTR_VALUE_DOENER))
                .option(sortOption);
    }

    private SearchMatcher search(final SearchFilter searchFilter) {
        return search(searchFilter.toString());
    }

    private SearchMatcher search(final String searchFilter) {
        return search(searchFilter, apiVersion);
    }

    private static SearchMatcher search(
            final String searchFilter,
            final JsonSchemaVersion apiVersion) {
        return searchThings(apiVersion)
                .filter(searchFilter)
                .option(newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC));
    }

}
