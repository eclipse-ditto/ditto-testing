/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation
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
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.featureDesiredProperty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.featureProperty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEmpty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.and;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.newSortOption;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.not;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.or;

import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.VersionedSearchIntegrationTest;
import org.eclipse.ditto.testing.common.matcher.search.SearchMatcher;
import org.eclipse.ditto.testing.common.matcher.search.SearchProperties;
import org.eclipse.ditto.testing.common.matcher.search.SortProperties;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.thingsearch.model.SearchFilter;
import org.eclipse.ditto.thingsearch.model.SortOptionEntry;
import org.junit.Test;

/**
 * Integration tests for the {@code empty()} RQL function covering all 5 empty semantics
 * (absent, null, empty array, empty object, empty string), negation, combinations with other
 * operators, and different field types (attributes, feature properties, desired properties).
 */
public class QueryThingsWithFilterByEmptyIT extends VersionedSearchIntegrationTest {

    private static final String TAGS_ATTR = "tags";
    private static final String LABEL_ATTR = "label";
    private static final String INFO_ATTR = "info";
    private static final String SENSOR_FEATURE = "sensor";
    private static final String VALUE_PROPERTY = "value";

    private static ThingId thing1Id;  // tags=["a","b"], feature with properties and desiredProperties
    private static ThingId thing2Id;  // tags=[] (empty array)
    private static ThingId thing3Id;  // tags={} (empty object)
    private static ThingId thing4Id;  // tags="" (empty string)
    private static ThingId thing5Id;  // tags=null
    private static ThingId thing6Id;  // tags absent
    private static ThingId thing7Id;  // feature with empty properties {}
    private static ThingId thing8Id;  // feature with no properties
    private static ThingId thing9Id;  // no features at all, tags=["x"]
    private static ThingId thing10Id; // feature with empty desiredProperties

    @Override
    protected void createTestData() {
        thing1Id = persistThingAndWaitTillAvailable(createThing1());
        thing2Id = persistThingAndWaitTillAvailable(createThing2());
        thing3Id = persistThingAndWaitTillAvailable(createThing3());
        thing4Id = persistThingAndWaitTillAvailable(createThing4());
        thing5Id = persistThingAndWaitTillAvailable(createThing5());
        thing6Id = persistThingAndWaitTillAvailable(createThing6());
        thing7Id = persistThingAndWaitTillAvailable(createThing7());
        thing8Id = persistThingAndWaitTillAvailable(createThing8());
        thing9Id = persistThingAndWaitTillAvailable(createThing9());
        thing10Id = persistThingAndWaitTillAvailable(createThing10());
    }

    private Thing createThing1() {
        final FeatureProperties props = FeatureProperties.newBuilder()
                .set(VALUE_PROPERTY, 42)
                .set("status", "ok")
                .build();
        final FeatureProperties desiredProps = FeatureProperties.newBuilder()
                .set(VALUE_PROPERTY, 50)
                .build();
        return Thing.newBuilder()
                .setId(createThingId(1))
                .setAttributes(Attributes.newBuilder()
                        .set(TAGS_ATTR, JsonArray.newBuilder().add("a").add("b").build())
                        .set(LABEL_ATTR, "full")
                        .set(INFO_ATTR, JsonObject.newBuilder().set("nested", "val").build())
                        .build())
                .setFeature(Feature.newBuilder()
                        .properties(props)
                        .desiredProperties(desiredProps)
                        .withId(SENSOR_FEATURE)
                        .build())
                .build();
    }

    private Thing createThing2() {
        return Thing.newBuilder()
                .setId(createThingId(2))
                .setAttributes(Attributes.newBuilder()
                        .set(TAGS_ATTR, JsonArray.empty())
                        .set(LABEL_ATTR, "empty-arr")
                        .build())
                .setFeature(SENSOR_FEATURE, FeatureProperties.newBuilder().set(VALUE_PROPERTY, 0).build())
                .build();
    }

    private Thing createThing3() {
        return Thing.newBuilder()
                .setId(createThingId(3))
                .setAttributes(Attributes.newBuilder()
                        .set(TAGS_ATTR, JsonObject.empty())
                        .set(LABEL_ATTR, "empty-obj")
                        .build())
                .setFeature(SENSOR_FEATURE, FeatureProperties.newBuilder().set(VALUE_PROPERTY, 0).build())
                .build();
    }

    private Thing createThing4() {
        return Thing.newBuilder()
                .setId(createThingId(4))
                .setAttributes(Attributes.newBuilder()
                        .set(TAGS_ATTR, JsonValue.of(""))
                        .set(LABEL_ATTR, "empty-str")
                        .build())
                .setFeature(SENSOR_FEATURE, FeatureProperties.newBuilder().set(VALUE_PROPERTY, 0).build())
                .build();
    }

    private Thing createThing5() {
        return Thing.newBuilder()
                .setId(createThingId(5))
                .setAttributes(Attributes.newBuilder()
                        .set(TAGS_ATTR, JsonFactory.nullLiteral())
                        .set(LABEL_ATTR, "null-val")
                        .build())
                .setFeature(SENSOR_FEATURE, FeatureProperties.newBuilder().set(VALUE_PROPERTY, 0).build())
                .build();
    }

    private Thing createThing6() {
        return Thing.newBuilder()
                .setId(createThingId(6))
                .setAttributes(Attributes.newBuilder()
                        .set(LABEL_ATTR, "absent")
                        .build())
                .setFeature(SENSOR_FEATURE, FeatureProperties.newBuilder().set(VALUE_PROPERTY, 0).build())
                .build();
    }

    private Thing createThing7() {
        return Thing.newBuilder()
                .setId(createThingId(7))
                .setAttributes(Attributes.newBuilder()
                        .set(LABEL_ATTR, "empty-props")
                        .build())
                .setFeature(Feature.newBuilder()
                        .properties(FeatureProperties.newBuilder().build())
                        .withId(SENSOR_FEATURE)
                        .build())
                .build();
    }

    private Thing createThing8() {
        return Thing.newBuilder()
                .setId(createThingId(8))
                .setAttributes(Attributes.newBuilder()
                        .set(LABEL_ATTR, "no-props")
                        .build())
                .setFeature(Feature.newBuilder()
                        .withId(SENSOR_FEATURE)
                        .build())
                .build();
    }

    private Thing createThing9() {
        return Thing.newBuilder()
                .setId(createThingId(9))
                .setAttributes(Attributes.newBuilder()
                        .set(LABEL_ATTR, "no-features")
                        .set(TAGS_ATTR, JsonArray.newBuilder().add("x").build())
                        .build())
                .build();
    }

    private Thing createThing10() {
        return Thing.newBuilder()
                .setId(createThingId(10))
                .setAttributes(Attributes.newBuilder()
                        .set(LABEL_ATTR, "desired-empty")
                        .build())
                .setFeature(Feature.newBuilder()
                        .properties(FeatureProperties.newBuilder().set(VALUE_PROPERTY, 99).build())
                        .desiredProperties(FeatureProperties.newBuilder().build())
                        .withId(SENSOR_FEATURE)
                        .build())
                .build();
    }

    private static ThingId createThingId(final int i) {
        return ThingId.of(idGenerator()
                .withPrefixedRandomName(QueryThingsWithFilterByEmptyIT.class.getSimpleName(), String.valueOf(i)));
    }

    @Test
    public void queryByEmptyAttributeTags() {
        search(attribute(TAGS_ATTR).empty())
                .expectingBody(isEqualTo(toThingResult(thing10Id, thing2Id, thing3Id, thing4Id,
                        thing5Id, thing6Id, thing7Id, thing8Id)))
                .fire();
    }

    @Test
    public void queryByNotEmptyAttributeTags() {
        search(not(attribute(TAGS_ATTR).empty()))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing9Id)))
                .fire();
    }

    @Test
    public void queryByEmptyNonEmptyStringAttribute() {
        search(attribute(LABEL_ATTR).empty())
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void queryByNotEmptyNestedAttribute() {
        search(not(attribute(INFO_ATTR).empty()))
                .expectingBody(isEqualTo(toThingResult(thing1Id)))
                .fire();
    }

    @Test
    public void queryByEmptyFeaturePropertyValue() {
        search(featureProperty(SENSOR_FEATURE, VALUE_PROPERTY).empty())
                .expectingBody(isEqualTo(toThingResult(thing7Id, thing8Id, thing9Id)))
                .fire();
    }

    @Test
    public void queryByNotEmptyFeaturePropertyValue() {
        search(not(featureProperty(SENSOR_FEATURE, VALUE_PROPERTY).empty()))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing10Id, thing2Id, thing3Id,
                        thing4Id, thing5Id, thing6Id)))
                .fire();
    }

    @Test
    public void queryByNotEmptyDesiredProperty() {
        if (apiVersion == JsonSchemaVersion.V_2) {
            search(not(featureDesiredProperty(SENSOR_FEATURE, VALUE_PROPERTY).empty()))
                    .expectingBody(isEqualTo(toThingResult(thing1Id)))
                    .fire();
        }
    }

    @Test
    public void queryByEmptyWithZeroValueIsNotEmpty() {
        search(and(
                attribute(LABEL_ATTR).eq("empty-arr"),
                featureProperty(SENSOR_FEATURE, VALUE_PROPERTY).empty()
        ))
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void queryByEmptyOrEq() {
        search(or(
                attribute(TAGS_ATTR).empty(),
                attribute(TAGS_ATTR).eq("a")
        ))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing10Id, thing2Id, thing3Id,
                        thing4Id, thing5Id, thing6Id, thing7Id, thing8Id)))
                .fire();
    }

    @Test
    public void queryByNotEmptyAndGt() {
        search(and(
                not(featureProperty(SENSOR_FEATURE, VALUE_PROPERTY).empty()),
                featureProperty(SENSOR_FEATURE, VALUE_PROPERTY).gt(10)
        ))
                .expectingBody(isEqualTo(toThingResult(thing1Id, thing10Id)))
                .fire();
    }

    @Test
    public void queryByExistsAndEmpty() {
        search(and(
                attribute(TAGS_ATTR).exists(),
                attribute(TAGS_ATTR).empty()
        ))
                .expectingBody(isEqualTo(toThingResult(thing2Id, thing3Id, thing4Id, thing5Id)))
                .fire();
    }

    @Test
    public void queryByEmptyNonExistentFeatureProperty() {
        search(and(
                attribute(LABEL_ATTR).eq("full"),
                featureProperty("nonexistent", VALUE_PROPERTY).empty()
        ))
                .expectingBody(isEqualTo(toThingResult(thing1Id)))
                .fire();
    }

    private SearchMatcher search(final SearchFilter searchFilter) {
        return search(searchFilter, apiVersion);
    }

    private static SearchMatcher search(final SearchFilter searchFilter, final JsonSchemaVersion apiVersion) {
        return searchThings(apiVersion)
                .filter(and(searchFilter, SearchProperties.thingId().like("*QueryThingsWithFilterByEmptyIT*")))
                .option(newSortOption(SortProperties.thingId(), SortOptionEntry.SortOrder.ASC));
    }

}
