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
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.and;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.not;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSortOption;

import java.util.List;

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.testing.common.matcher.search.SearchProperties;
import org.eclipse.ditto.testing.common.matcher.search.SortProperties;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SearchProperty;
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

    private static final SearchFilter THING_ID_FILTER =
            SearchProperties.thingId().like("*QueryThingsWithFilterByFeaturesIT*");
    // in() operands of the null-operand tests; "non-null" is thing2's NULL_PROPERTY value (see createThing2)
    private static final List<JsonValue> NON_NULL_OR_NULL =
            List.of(JsonFactory.newValue("non-null"), JsonFactory.nullLiteral());
    private static final List<JsonValue> ONLY_NULL = List.of(JsonFactory.nullLiteral());

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
     * Test to check feature existence by feature ID.
     */
    @Test
    public void queryByFeatureId() {
        search(feature(FEATURE_ID1).exists())
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                .fire();
    }

    @Test
    public void queryByEmptyFeatureProperty() {
        search(featureProperty(FEATURE_ID1, STRING_PROPERTY).empty())
                .expectingBody(isEqualTo(toThingResult(thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByNotEmptyFeatureProperty() {
        search(not(featureProperty(FEATURE_ID1, STRING_PROPERTY).empty()))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                .fire();
    }

    @Test
    public void queryByEmptyNullFeatureProperty() {
        search(featureProperty(FEATURE_ID1, NULL_PROPERTY).empty())
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                .fire();
    }

    @Test
    public void queryByNotEmptyNullFeatureProperty() {
        search(not(featureProperty(FEATURE_ID1, NULL_PROPERTY).empty()))
                .expectingBody(isEqualTo(toThingResult(thing2Id)))
                .fire();
    }

    @Test
    public void queryByEmptyDesiredProperty() {
        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(FEATURE_ID1, STRING_PROPERTY).empty())
                    .expectingBody(isEqualTo(toThingResult(thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByNotEmptyDesiredProperty() {
        if (apiVersion == JsonSchemaVersion.V_2) {
            search(not(featureDesiredProperty(FEATURE_ID1, STRING_PROPERTY).empty()))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id)))
                    .fire();
        }
    }

    /**
     * Test to check query by specific feature and property.
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

    /*
     * Null operands of in()/ge/gt/le/lt. The search contract: a null operand of in(), ge and le matches a null value
     * and a missing property; gt and lt with a null operand match nothing; only eq(null) and ne require the property
     * to exist. On the feature wildcard a feature that lacks the property matches the null-or-missing operators.
     * This is the behaviour of the MongoDB-backed search the contract comes from. Fixture: thing1 nullProperty =
     * null, thing2 "non-null" (on both features), thing3/thing4 features without nullProperty.
     */

    @Test
    public void queryByInStringAndNull() {
        search(featureProperty(NULL_PROPERTY).in(NON_NULL_OR_NULL))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(NULL_PROPERTY).in(NON_NULL_OR_NULL))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByInStringAndNullOnCertainFeature() {
        search(featureProperty(FEATURE_ID1, NULL_PROPERTY).in(NON_NULL_OR_NULL))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(FEATURE_ID1, NULL_PROPERTY).in(NON_NULL_OR_NULL))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByInNull() {
        search(featureProperty(NULL_PROPERTY).in(ONLY_NULL))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(NULL_PROPERTY).in(ONLY_NULL))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByInNullOnCertainFeature() {
        search(featureProperty(FEATURE_ID1, NULL_PROPERTY).in(ONLY_NULL))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(featureDesiredProperty(FEATURE_ID1, NULL_PROPERTY).in(ONLY_NULL))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    /**
     * The null-or-missing leg of in() combined with a point predicate on another property of the same feature.
     */
    @Test
    public void queryByInStringAndNullCombinedWithEqString() {
        search(and(featureProperty(NULL_PROPERTY).in(NON_NULL_OR_NULL), featureProperty(STRING_PROPERTY).eq("c")))
                .expectingBody(isEqualTo(toThingResult(thing3Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(and(featureDesiredProperty(NULL_PROPERTY).in(NON_NULL_OR_NULL),
                    featureDesiredProperty(STRING_PROPERTY).eq("c")))
                    .expectingBody(isEqualTo(toThingResult(thing3Id)))
                    .fire();
        }
    }

    /**
     * Negated in() with a value and a null operand: every thing holds the value, null or no such property at all, so
     * nothing is left.
     */
    @Test
    public void queryByNotInStringAndNull() {
        search(not(featureProperty(NULL_PROPERTY).in(NON_NULL_OR_NULL)))
                .expectingBody(isEmpty())
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(not(featureDesiredProperty(NULL_PROPERTY).in(NON_NULL_OR_NULL)))
                    .expectingBody(isEmpty())
                    .fire();
        }
    }

    @Test
    public void queryByNotInStringAndNullOnCertainFeature() {
        search(not(featureProperty(FEATURE_ID1, NULL_PROPERTY).in(NON_NULL_OR_NULL)))
                .expectingBody(isEmpty())
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(not(featureDesiredProperty(FEATURE_ID1, NULL_PROPERTY).in(NON_NULL_OR_NULL)))
                    .expectingBody(isEmpty())
                    .fire();
        }
    }

    @Test
    public void queryByGeNull() {
        search(nullComparison("ge", featureProperty(NULL_PROPERTY)))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(nullComparison("ge", featureDesiredProperty(NULL_PROPERTY)))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByGeNullOnCertainFeature() {
        search(nullComparison("ge", featureProperty(FEATURE_ID1, NULL_PROPERTY)))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(nullComparison("ge", featureDesiredProperty(FEATURE_ID1, NULL_PROPERTY)))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByLeNull() {
        search(nullComparison("le", featureProperty(NULL_PROPERTY)))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(nullComparison("le", featureDesiredProperty(NULL_PROPERTY)))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByLeNullOnCertainFeature() {
        search(nullComparison("le", featureProperty(FEATURE_ID1, NULL_PROPERTY)))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(nullComparison("le", featureDesiredProperty(FEATURE_ID1, NULL_PROPERTY)))
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByGtNull() {
        search(nullComparison("gt", featureProperty(NULL_PROPERTY)))
                .expectingBody(isEmpty())
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(nullComparison("gt", featureDesiredProperty(NULL_PROPERTY)))
                    .expectingBody(isEmpty())
                    .fire();
        }
    }

    @Test
    public void queryByGtNullOnCertainFeature() {
        search(nullComparison("gt", featureProperty(FEATURE_ID1, NULL_PROPERTY)))
                .expectingBody(isEmpty())
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(nullComparison("gt", featureDesiredProperty(FEATURE_ID1, NULL_PROPERTY)))
                    .expectingBody(isEmpty())
                    .fire();
        }
    }

    @Test
    public void queryByLtNull() {
        search(nullComparison("lt", featureProperty(NULL_PROPERTY)))
                .expectingBody(isEmpty())
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(nullComparison("lt", featureDesiredProperty(NULL_PROPERTY)))
                    .expectingBody(isEmpty())
                    .fire();
        }
    }

    @Test
    public void queryByLtNullOnCertainFeature() {
        search(nullComparison("lt", featureProperty(FEATURE_ID1, NULL_PROPERTY)))
                .expectingBody(isEmpty())
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search(nullComparison("lt", featureDesiredProperty(FEATURE_ID1, NULL_PROPERTY)))
                    .expectingBody(isEmpty())
                    .fire();
        }
    }

    @Test
    public void queryByNotGtNull() {
        search("not(" + nullComparison("gt", featureProperty(NULL_PROPERTY)) + ")")
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search("not(" + nullComparison("gt", featureDesiredProperty(NULL_PROPERTY)) + ")")
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByNotGtNullOnCertainFeature() {
        search("not(" + nullComparison("gt", featureProperty(FEATURE_ID1, NULL_PROPERTY)) + ")")
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search("not(" + nullComparison("gt", featureDesiredProperty(FEATURE_ID1, NULL_PROPERTY)) + ")")
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByNotLtNull() {
        search("not(" + nullComparison("lt", featureProperty(NULL_PROPERTY)) + ")")
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search("not(" + nullComparison("lt", featureDesiredProperty(NULL_PROPERTY)) + ")")
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    @Test
    public void queryByNotLtNullOnCertainFeature() {
        search("not(" + nullComparison("lt", featureProperty(FEATURE_ID1, NULL_PROPERTY)) + ")")
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                .fire();

        if (apiVersion == JsonSchemaVersion.V_2) {
            search("not(" + nullComparison("lt", featureDesiredProperty(FEATURE_ID1, NULL_PROPERTY)) + ")")
                    .expectingBody(isEqualTo(toThingResult(thing1Id, thing2Id, thing3Id, thing4Id)))
                    .fire();
        }
    }

    /**
     * Renders {@code <operator>(<path>,null)} as raw RQL: {@link SearchProperty#ge(String)}, {@code gt}, {@code le}
     * and {@code lt} reject a {@code null} value, so the filter cannot be built with the search model.
     */
    private static String nullComparison(final String operator, final SearchProperty property) {
        return operator + "(" + property.getPath() + ",null)";
    }

    private SearchMatcher search(final SearchFilter searchFilter) {
        return search(searchFilter, apiVersion);
    }

    private SearchMatcher search(final String rqlFilter) {
        return searchThings(apiVersion)
                .filter("and(" + rqlFilter + "," + THING_ID_FILTER + ")")
                .option(newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC));
    }

    private static SearchMatcher search(final SearchFilter searchFilter, final JsonSchemaVersion apiVersion) {
        return searchThings(apiVersion)
                .filter(and(searchFilter, THING_ID_FILTER))
                .option(newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC));
    }

}
