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


import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.feature;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.featureDesiredProperty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.featureProperty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEmpty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSortOption;

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.testing.common.matcher.search.SortProperties;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SortOptionEntry;
import org.junit.Test;

/**
 *
 */
public class QueryThingsWithFilterByFeaturesIT extends VersionedSearchIntegrationTest {

    private static final String NUMBER_PROPERTY = "numberProperty";
    private static final String STRING_PROPERTY = "stringProperty";
    private static final String REGEX_PROPERTY = "stringRegex";
    private static final String BOOL_PROPERTY = "boolProperty";
    private static final String NULL_PROPERTY = "nullProperty";

    private static final String THING1_STRING_PROPERTY_VALUE = "a";
    private static final String THING1_REGEX_VALUE = "Das ist ein X beliebiger String";
    private static final String THING1_REGEX_STARTS_WITH = "Das*";

    private static final String THING2_STRING_PROPERTY_VALUE = "b";
    private static final int THING2_NUMBER_PROPERTY_VALUE = 2;
    private static final boolean THING2_BOOL_PROPERTY_VALUE = true;
    private static final String THING2_REGEX_VALUE = "Der zweite String";
    private static final String THING2_REGEX_CONTAINS = "*zweite*";

    private static final double THING3_NUMBER_PROPERTY_VALUE = 3.1;
    private static final boolean THING3_BOOL_PROPERTY_VALUE = false;
    private static final String THING3_REGEX_VALUE = "Teststring nummer drei";
    // wildcard "*" in the middle of a value
    private static final String THING3_WILDCARD_ASTERISK_MIDDLE_VALUE = "Teststr*ng nummer drei";
    private static final String THING3_WILDCARD_QUESTION_MARK_VALUE = "Teststr?ng nummer drei";
    private static final String THING3_WILDCARD_DOT_VALUE = "Teststr.ng nummer drei";
    private static final String THING3_WILDCARD_BOL_VALUE = "^Teststring nummer drei";
    private static final String THING3_WILDCARD_EOL_VALUE = "Teststring nummer drei$";

    private static final boolean THING4_BOOL_PROPERTY_VALUE = THING3_BOOL_PROPERTY_VALUE;
    private static final String THING4_REGEX_VALUE = "Der vierte und letzte Teststring";
    private static final String THING4_REGEX_ENDS_WITH = "*Teststring";

    private static final String FEATURE_ID1 = "testFeature";
    private static final String ANOTHER_FEATURE_ID = "anotherFeature";
    private static final String NOT_SEARCHED_FEATURE_ID = "notSearchedFeature";

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
        final FeatureProperties properties = FeatureProperties.newBuilder()
                .set(STRING_PROPERTY, THING1_STRING_PROPERTY_VALUE)
                .set(NUMBER_PROPERTY, 1)
                .set(BOOL_PROPERTY, true)
                .set(NULL_PROPERTY, JsonFactory.nullLiteral())
                .set(REGEX_PROPERTY, THING1_REGEX_VALUE)
                .build();
        final Feature feature1 = Feature.newBuilder()
                .properties(properties)
                .desiredProperties(properties)
                .withId(FEATURE_ID1)
                .build();
        return Thing.newBuilder()
                .setId(createThingId(1))
                .setFeature(feature1)
                .build();
    }

    private Thing createThing2() {
        final FeatureProperties featureProperties = FeatureProperties.newBuilder()
                .set(STRING_PROPERTY, THING2_STRING_PROPERTY_VALUE)
                .set(NUMBER_PROPERTY, THING2_NUMBER_PROPERTY_VALUE)
                .set(BOOL_PROPERTY, THING2_BOOL_PROPERTY_VALUE)
                .set(NULL_PROPERTY, "non-null")
                .set(REGEX_PROPERTY, THING2_REGEX_VALUE)
                .build();
        final Feature feature1 = Feature.newBuilder()
                .properties(featureProperties)
                .desiredProperties(featureProperties)
                .withId(FEATURE_ID1)
                .build();
        final Feature anotherFeature = Feature.newBuilder()
                .properties(featureProperties)
                .desiredProperties(featureProperties)
                .withId(ANOTHER_FEATURE_ID)
                .build();
        return Thing.newBuilder()
                .setId(createThingId(2))
                .setFeature(feature1)
                .setFeature(anotherFeature)
                .build();
    }

    private Thing createThing3() {
        final FeatureProperties properties = FeatureProperties.newBuilder()
                .set(STRING_PROPERTY, "c")
                .set(NUMBER_PROPERTY, THING3_NUMBER_PROPERTY_VALUE)
                .set(BOOL_PROPERTY, THING3_BOOL_PROPERTY_VALUE)
                .set(REGEX_PROPERTY, THING3_REGEX_VALUE)
                .build();
        final Feature notSearchedFeature = Feature.newBuilder()
                .properties(properties)
                .desiredProperties(properties)
                .withId(NOT_SEARCHED_FEATURE_ID)
                .build();
        return Thing.newBuilder()
                .setId(createThingId(3))
                .setFeature(notSearchedFeature)
                .build();
    }

    private Thing createThing4() {
        final FeatureProperties properties = FeatureProperties.newBuilder()
                .set(STRING_PROPERTY, "d")
                .set(NUMBER_PROPERTY, 4)
                .set(BOOL_PROPERTY, THING4_BOOL_PROPERTY_VALUE)
                .set(REGEX_PROPERTY, THING4_REGEX_VALUE)
                .build();
        final Feature notSearchedFeature = Feature.newBuilder()
                .properties(properties)
                .desiredProperties(properties)
                .withId(NOT_SEARCHED_FEATURE_ID)
                .build();
        return Thing.newBuilder()
                .setId(createThingId(4))
                .setFeature(notSearchedFeature)
                .build();
    }

    private static ThingId createThingId(final int i) {
        return ThingId.of(idGenerator()
                .withPrefixedRandomName(QueryThingsWithFilterByFeaturesIT.class.getSimpleName(), String.valueOf(i)));
    }

    @Test
    public void queryByEqNumber() {
        search(featureProperty(NUMBER_PROPERTY).eq(THING2_NUMBER_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing2Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(NUMBER_PROPERTY).eq(THING2_NUMBER_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing2Id)))
                    .fire();
        }
    }

    @Test
    public void queryByEqBoolean() {
        search(featureProperty(BOOL_PROPERTY).eq(THING3_BOOL_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(BOOL_PROPERTY).eq(THING3_BOOL_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByEqString() {
        search(featureProperty(STRING_PROPERTY).eq(THING1_STRING_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(STRING_PROPERTY).eq(THING1_STRING_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id)))
                    .fire();
        }
    }

    @Test
    public void queryByEqNull() {
        search(featureProperty(NULL_PROPERTY).eq( null))
                .expectingBody(isEqualTo(toThingResult(thing1Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(NULL_PROPERTY).eq(null))
                    .expectingBody(isEqualTo(toThingResult(thing1Id)))
                    .fire();
        }
    }

    @Test
    public void queryByNeString() {
        search(featureProperty(STRING_PROPERTY).ne(THING1_STRING_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing2Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(STRING_PROPERTY).ne(THING1_STRING_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing2Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByNeNumber() {
        search(featureProperty(NUMBER_PROPERTY).ne(THING2_NUMBER_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(NUMBER_PROPERTY).ne(THING2_NUMBER_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByNeBoolean() {
        search(featureProperty(BOOL_PROPERTY).ne(THING3_BOOL_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(BOOL_PROPERTY).ne(THING3_BOOL_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                    .fire();
        }
    }

    @Test
    public void queryByNeNull() {
        search(featureProperty(NULL_PROPERTY).ne(null))
                .expectingBody(isEqualTo(toThingResult(thing2Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(NULL_PROPERTY).ne(null))
                    .expectingBody(isEqualTo(toThingResult(thing2Id)))
                    .fire();
        }
    }

    @Test
    public void queryByGtString() {
        search(featureProperty(STRING_PROPERTY).gt(THING2_STRING_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(STRING_PROPERTY).gt(THING2_STRING_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByGtNumber() {
        search(featureProperty(NUMBER_PROPERTY).gt(THING3_NUMBER_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(NUMBER_PROPERTY).gt(THING3_NUMBER_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByGtBoolean() {
        search(featureProperty(BOOL_PROPERTY).gt(THING3_BOOL_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(BOOL_PROPERTY).gt(THING3_BOOL_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                    .fire();
        }
    }

    @Test
    public void queryByGeString() {
        search(featureProperty(STRING_PROPERTY).ge(THING2_STRING_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing2Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(STRING_PROPERTY).ge(THING2_STRING_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing2Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByGeNumber() {
        search(featureProperty(NUMBER_PROPERTY).ge(THING3_NUMBER_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(NUMBER_PROPERTY).ge(THING3_NUMBER_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByGeBoolean() {
        search(featureProperty(BOOL_PROPERTY).ge(THING3_BOOL_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(BOOL_PROPERTY).ge(THING3_BOOL_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByLtString() {
        search(featureProperty(STRING_PROPERTY).lt(THING2_STRING_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(STRING_PROPERTY).lt(THING2_STRING_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id)))
                    .fire();
        }
    }

    @Test
    public void queryByLtNumber() {
        search(featureProperty(NUMBER_PROPERTY).lt(THING3_NUMBER_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(NUMBER_PROPERTY).lt(THING3_NUMBER_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                    .fire();
        }
    }

    @Test
    public void queryByLtBoolean() {
        search(featureProperty(BOOL_PROPERTY).lt(THING2_BOOL_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(BOOL_PROPERTY).lt(THING2_BOOL_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByLeString() {
        search(featureProperty(STRING_PROPERTY).le(THING2_STRING_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(STRING_PROPERTY).le(THING2_STRING_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                    .fire();
        }
    }

    @Test
    public void queryByLeNumber() {
        search(featureProperty(NUMBER_PROPERTY).le(THING3_NUMBER_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(NUMBER_PROPERTY).le(THING3_NUMBER_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id)))
                    .fire();
        }
    }

    @Test
    public void queryByLeBoolean() {
        search(featureProperty(BOOL_PROPERTY).le(THING2_BOOL_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(BOOL_PROPERTY).le(THING2_BOOL_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByInNumber() {
        search(featureProperty(NUMBER_PROPERTY).in(THING2_NUMBER_PROPERTY_VALUE, THING3_NUMBER_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing2Id, thing3Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(NUMBER_PROPERTY).in(THING2_NUMBER_PROPERTY_VALUE,
                    THING3_NUMBER_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing2Id, thing3Id)))
                    .fire();
        }
    }

    @Test
    public void queryByInBoolean() {
        search(featureProperty(BOOL_PROPERTY).in(THING2_BOOL_PROPERTY_VALUE, THING3_BOOL_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(BOOL_PROPERTY).in(THING2_BOOL_PROPERTY_VALUE, THING3_BOOL_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByInString() {
        search(featureProperty(STRING_PROPERTY).in(THING1_STRING_PROPERTY_VALUE, THING2_STRING_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(STRING_PROPERTY).in(THING1_STRING_PROPERTY_VALUE,
                    THING2_STRING_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                    .fire();
        }
    }

    /**
     * Test to check starts with functionality
     */
    @Test
    public void queryByLikeString1() {
        search(featureProperty(REGEX_PROPERTY).like(THING1_REGEX_STARTS_WITH))
                .expectingBody(isEqualTo(toThingResult(thing1Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(REGEX_PROPERTY).like(THING1_REGEX_STARTS_WITH))
                    .expectingBody(isEqualTo(toThingResult(thing1Id)))
                    .fire();
        }
    }

    /**
     * Test to check contains functionality
     */
    @Test
    public void queryByLikeString2() {
        search(featureProperty(REGEX_PROPERTY).like(THING2_REGEX_CONTAINS))
                .expectingBody(isEqualTo(toThingResult(thing2Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(REGEX_PROPERTY).like(THING2_REGEX_CONTAINS))
                    .expectingBody(isEqualTo(toThingResult(thing2Id)))
                    .fire();
        }
    }

    /**
     * Make sure that all supported wildcards can be used.
     */
    @Test
    public void queryByLikeString3WithSupportedWildcards() {
        search(featureProperty(REGEX_PROPERTY).like(THING3_WILDCARD_QUESTION_MARK_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing3Id)))
                .fire();

        search(featureProperty(REGEX_PROPERTY).like(THING3_WILDCARD_ASTERISK_MIDDLE_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing3Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(REGEX_PROPERTY).like(THING3_WILDCARD_QUESTION_MARK_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing3Id)))
                    .fire();

            search(featureDesiredProperty(REGEX_PROPERTY).like(THING3_WILDCARD_ASTERISK_MIDDLE_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing3Id)))
                    .fire();
        }
    }

    /**
     * Make sure that not supported wildcards can't be used.
     */
    @Test
    public void queryByLikeString3WithNotSupportedWildcards() {
        search(featureProperty(REGEX_PROPERTY).like(THING3_WILDCARD_DOT_VALUE))
                .expectingBody(isEmpty())
                .fire();

        search(featureProperty(REGEX_PROPERTY).like(THING3_WILDCARD_BOL_VALUE))
                .expectingBody(isEmpty())
                .fire();

        search(featureProperty(REGEX_PROPERTY).like(THING3_WILDCARD_EOL_VALUE))
                .expectingBody(isEmpty())
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(REGEX_PROPERTY).like(THING3_WILDCARD_DOT_VALUE))
                    .expectingBody(isEmpty())
                    .fire();

            search(featureDesiredProperty(REGEX_PROPERTY).like(THING3_WILDCARD_BOL_VALUE))
                    .expectingBody(isEmpty())
                    .fire();

            search(featureDesiredProperty(REGEX_PROPERTY).like(THING3_WILDCARD_EOL_VALUE))
                    .expectingBody(isEmpty())
                    .fire();
        }
    }

    /**
     * Test to check ends with functionality
     */
    @Test
    public void queryByLikeString4() {
        search(featureProperty(REGEX_PROPERTY).like(THING4_REGEX_ENDS_WITH))
                .expectingBody(isEqualTo(toThingResult(thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(REGEX_PROPERTY).like(THING4_REGEX_ENDS_WITH))
                    .expectingBody(isEqualTo(toThingResult(thing4Id)))
                    .fire();
        }
    }

    /**
     * Test to check ends with functionality
     */
    @Test
    public void queryByFeatureId() {
        search(feature(FEATURE_ID1).exists())
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                .fire();

    }

    /**
     * Test to check ends with functionality
     */
    @Test
    public void queryByEqOnCertainFeatureAndProperty() {
        search(featureProperty(ANOTHER_FEATURE_ID, STRING_PROPERTY).eq(THING2_STRING_PROPERTY_VALUE))
                .expectingBody(isEqualTo(toThingResult(thing2Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(ANOTHER_FEATURE_ID, STRING_PROPERTY).eq(THING2_STRING_PROPERTY_VALUE))
                    .expectingBody(isEqualTo(toThingResult(thing2Id)))
                    .fire();
        }
    }

    private SearchMatcher search(final SearchFilter searchFilter) {
        return search(searchFilter, apiVersion);
    }

    private static SearchMatcher search(final SearchFilter searchFilter, final JsonSchemaVersion apiVersion) {
        return searchThings(apiVersion).filter(searchFilter)
                .option(newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC));
    }

}
