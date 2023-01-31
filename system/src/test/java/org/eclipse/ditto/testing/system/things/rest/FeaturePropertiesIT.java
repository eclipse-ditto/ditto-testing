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
package org.eclipse.ditto.testing.system.things.rest;

import static org.hamcrest.core.IsEqual.equalTo;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.HttpParameter;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ResourcePathBuilder;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.ThingJsonProducer;
import org.eclipse.ditto.testing.common.UriEncoding;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.things.model.Features;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration test for the Properties REST API.
 */
public final class FeaturePropertiesIT extends IntegrationTest {

    /**
     * Property key with lots of special chars. NOTE: it does not contain slash, because finding such properties is not
     * supported currently.
     */
    private static final String SPECIAL_CHARS_KEY = "!\\\"#$%&'()*+,:;=?@[]{|} aZ0";
    private static final String INVALID_PATH = "foo/ä-is-invalid/bar";

    @Test
    public void putGetAndDeleteProperty() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";
        final String featuresJson = "{\"" + featureId + "\":{}}";
        final String propertyJson = "{\"bar\":\"baz\"}";

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        putProperty(TestConstants.API_V_2, thingId, featureId, "foo", propertyJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getProperty(TestConstants.API_V_2, thingId, featureId, "foo")
                .expectingBody(containsOnlyJsonKey("bar"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteProperty(TestConstants.API_V_2, thingId, featureId, "foo")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void putGetAndDeleteNestedProperty() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";
        final String featuresJson = "{\"" + featureId + "\":{}}";
        final String propertyJson = "{\"villain\":\"gargamel\"}";

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        putProperty(TestConstants.API_V_2, thingId, featureId, "foo/bar/baz", propertyJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getProperty(TestConstants.API_V_2, thingId, featureId, "foo/bar/baz")
                .expectingBody(containsOnlyJsonKey("villain"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteProperty(TestConstants.API_V_2, thingId, featureId, "foo/bar/baz")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void crudPropertyWithSpecialChars() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";
        final JsonObject featuresJson = JsonFactory.newObjectBuilder()
                .set("properties", JsonFactory.newObjectBuilder().set(SPECIAL_CHARS_KEY, "baz").build())
                .build();

        putFeature(TestConstants.API_V_2, thingId, featureId, featuresJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final String encodedPropKey = UriEncoding.encodePathSegment(SPECIAL_CHARS_KEY);

        getProperty(TestConstants.API_V_2, thingId, featureId, encodedPropKey)
                .expectingBody(equalTo("\"baz\""))
                .expectingHttpStatus(HttpStatus.OK)
                .disableUrlEncoding()
                .fire();

        putProperty(TestConstants.API_V_2, thingId, featureId, encodedPropKey, "\"bar\"")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .disableUrlEncoding()
                .fire();

        getProperty(TestConstants.API_V_2, thingId, featureId, encodedPropKey)
                .expectingBody(equalTo("\"bar\""))
                .expectingHttpStatus(HttpStatus.OK)
                .disableUrlEncoding()
                .fire();

        deleteProperty(TestConstants.API_V_2, thingId, featureId, encodedPropKey)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .disableUrlEncoding()
                .fire();
    }

    @Test
    public void crudNestedPropertyWithSpecialChars() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";
        final JsonObject featureJson = JsonFactory.newObjectBuilder()
                .set("properties", JsonFactory.newObjectBuilder()
                        .set(SPECIAL_CHARS_KEY, JsonFactory.newObjectBuilder()
                                .set(SPECIAL_CHARS_KEY, "baz")
                                .build())
                        .build())
                .build();

        putFeature(TestConstants.API_V_2, thingId, featureId, featureJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final String encodedPropPath =
                UriEncoding.encodePathSegment(SPECIAL_CHARS_KEY) + "/" +
                        UriEncoding.encodePathSegment(SPECIAL_CHARS_KEY);

        getProperty(TestConstants.API_V_2, thingId, featureId, encodedPropPath)
                .expectingBody(equalTo("\"baz\""))
                .expectingHttpStatus(HttpStatus.OK)
                .disableUrlEncoding()
                .fire();

        putProperty(TestConstants.API_V_2, thingId, featureId, encodedPropPath, "\"bar\"")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .disableUrlEncoding()
                .fire();

        getProperty(TestConstants.API_V_2, thingId, featureId, encodedPropPath)
                .expectingBody(equalTo("\"bar\""))
                .expectingHttpStatus(HttpStatus.OK)
                .disableUrlEncoding()
                .fire();

        deleteProperty(TestConstants.API_V_2, thingId, featureId, encodedPropPath)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .disableUrlEncoding()
                .fire();
    }

    @Test
    public void tryCrudPropertyWithInvalidPropertyPath() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);
        final JsonObject featureJson = JsonFactory.newObjectBuilder()
                .set("properties", JsonFactory.newObjectBuilder().set("dummy", JsonFactory.newValue(42)).build())
                .build();

        final String featureId = "feature";
        putFeature(TestConstants.API_V_2, thingId, featureId, featureJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        putProperty(TestConstants.API_V_2, thingId, featureId, INVALID_PATH, "42")
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();

        putProperties(TestConstants.API_V_2, thingId, featureId,
                JsonObject.newBuilder()
                        .set("valid", JsonObject.newBuilder().set("not/valid", JsonValue.of(true)).build())
                        .build()
                        .toString())
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();

        getProperty(TestConstants.API_V_2, thingId, featureId, INVALID_PATH)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();

        deleteProperty(TestConstants.API_V_2, thingId, featureId, INVALID_PATH)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    @Test
    public void putGetAndDeleteNullProperty() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";
        final String featuresJson = "{\"" + featureId + "\":{}}";
        final String propertyJson = "null";

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        putProperty(TestConstants.API_V_2, thingId, featureId, "nullFoo", propertyJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getProperty(TestConstants.API_V_2, thingId, featureId, "nullFoo")
                .expectingBody(equalTo(propertyJson))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteProperty(TestConstants.API_V_2, thingId, featureId, "nullFoo")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void putAndGetPropertyTwice() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";
        final String featuresJson = "{\"" + featureId + "\":{}}";
        final String propertyJson = "{\"bar\":\"baz\"}";

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        putProperty(TestConstants.API_V_2, thingId, featureId, "foo", propertyJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getProperty(TestConstants.API_V_2, thingId, featureId, "foo")
                .expectingBody(containsOnlyJsonKey("bar"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final String propertyJsonToUpdate = "\"gargamel\"";

        putProperty(TestConstants.API_V_2, thingId, featureId, "foo", propertyJsonToUpdate)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getProperty(TestConstants.API_V_2, thingId, featureId, "foo")
                .expectingBody(equalTo(propertyJsonToUpdate))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteProperty(TestConstants.API_V_2, thingId, featureId, "foo")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void putAndGetNestedPropertyTwice() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";
        final String featuresJson = "{\"" + featureId + "\":{}}";
        final String propertyJson = "{\"gargamel\":{}}";

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        putProperty(TestConstants.API_V_2, thingId, featureId, "villain", propertyJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getProperty(TestConstants.API_V_2, thingId, featureId, "villain")
                .expectingBody(containsOnlyJsonKey("gargamel"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final String propertyJsonToUpdate = "\"azrael\"";

        putProperty(TestConstants.API_V_2, thingId, featureId, "villain/gargamel/pet", propertyJsonToUpdate)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getProperty(TestConstants.API_V_2, thingId, featureId, "villain/gargamel/pet")
                .expectingBody(equalTo(propertyJsonToUpdate))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteProperty(TestConstants.API_V_2, thingId, featureId, "villain")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void tryToGetPropertyOfUnknownThing() {
        final String thingId = serviceEnv.getDefaultNamespaceName() + ":unknown";
        final String featureId = "feature";

        getProperty(TestConstants.API_V_2, thingId, featureId, "villain")
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void tryToGetPropertyOfUnknownFeature() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";

        getProperty(TestConstants.API_V_2, thingId, featureId, "villain")
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void tryToGetUnknownProperty() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";
        final String featuresJson = "{\"" + featureId + "\":{}}";

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getProperty(TestConstants.API_V_2, thingId, featureId, "villain")
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void putGetAndDeleteNestedProperties() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";
        final String featuresJson = "{\"" + featureId + "\":{}}";
        final String propertiesJson = "{\"villain\":\"gargamel\"}";

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        putProperties(TestConstants.API_V_2, thingId, featureId, propertiesJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getProperties(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(containsOnlyJsonKey("villain"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteProperties(TestConstants.API_V_2, thingId, featureId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void putGetAndDeleteNullProperties() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";
        final String featuresJson = "{\"" + featureId + "\":{}}";
        final String propertiesJson = "null";

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        putProperties(TestConstants.API_V_2, thingId, featureId, propertiesJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getProperties(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(equalTo(propertiesJson))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteProperties(TestConstants.API_V_2, thingId, featureId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void putAndGetNestedPropertiesTwice() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";
        final String featuresJson = "{\"" + featureId + "\":{}}";
        final String propertiesJson = "{\"gargamel\":{}}";

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        putProperties(TestConstants.API_V_2, thingId, featureId, propertiesJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getProperties(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(containsOnlyJsonKey("gargamel"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final String propertiesJsonToUpdate = "{\"gargamel\":{\"pet\":\"azrael\"}}";

        putProperties(TestConstants.API_V_2, thingId, featureId, propertiesJsonToUpdate)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getProperties(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(equalTo(propertiesJsonToUpdate))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteProperties(TestConstants.API_V_2, thingId, featureId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void tryToGetPropertiesOfUnknownThing() {
        final String thingId = serviceEnv.getDefaultNamespaceName() + ":unknown";
        final String featureId = "feature";

        getProperties(TestConstants.API_V_2, thingId, featureId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void tryToGetPropertiesOfUnknownFeature() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";

        getProperties(TestConstants.API_V_2, thingId, featureId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void tryToGetUnknownProperties() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";
        final String featuresJson = "{\"" + featureId + "\":{}}";

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getProperties(TestConstants.API_V_2, thingId, featureId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    @Category(Acceptance.class)
    public void getPropertiesWithFieldSelectors() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";

        final String featuresJson = "{\"" + featureId + "\":{}}";
        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final String propertyJsonSimple = "42";
        final String propertyJsonComplex = "{\"foo\":{\"bar\":\"baz\"}}";

        putProperty(TestConstants.API_V_2, thingId, featureId, "simple", propertyJsonSimple)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        putProperty(TestConstants.API_V_2, thingId, featureId, "complex", propertyJsonComplex)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getThing(TestConstants.API_V_2, thingId).withParam(HttpParameter.FIELDS, "features/feature/properties(simple)")
                .expectingBody(containsOnlyJsonKey("features", "feature", "properties", "simple"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        getThing(TestConstants.API_V_2, thingId).withParam(HttpParameter.FIELDS, "features/feature/properties(complex)")
                .expectingBody(containsOnlyJsonKey("features", "feature", "properties", "complex", "foo", "bar"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .withParam(HttpParameter.FIELDS, "features/feature/properties(simple,complex)")
                .expectingBody(
                        containsOnlyJsonKey("features", "feature", "properties", "simple", "complex", "foo", "bar"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void getPropertiesWithFieldSelectorsContainingFeatureWildcard() {
        final Thing thing = new ThingJsonProducer().getThing();
        final ThingBuilder.FromCopy builder = thing.toBuilder();
        thing.getFeatures().stream().flatMap(Features::stream)
                .forEach(f -> builder.setFeature(f.getId() + "_2", f.getProperties().orElseThrow()));

        final String location = postThing(TestConstants.API_V_2, builder.build().toJson())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        getThing(TestConstants.API_V_2, thingId).withParam(HttpParameter.FIELDS, "features/*/properties/temperature")
                .expectingBody(contains(JsonObject.newBuilder()
                        .set(JsonPointer.of("features/EnvironmentScanner/properties/temperature"), 20.8)
                        .set(JsonPointer.of("features/EnvironmentScanner_2/properties/temperature"), 20.8)
                        .build()))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void tryToAccessApiWithDoubleSlashes() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "feature";

        final String featuresJson = "{\"" + featureId + "\":{}}";
        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final String path = ResourcePathBuilder.forThing(thingId).feature(featureId).properties().toString();
        final String jsonString = "{}";
        final String propertiesUrlWithDoubleSlash = thingsServiceUrl(TestConstants.API_V_2, path) + "//";
        final String propertiesUrlWithDoubleSlashAndId =
                propertiesUrlWithDoubleSlash + "bar";

        // no double slash merging -> tries to post against empty properties id
        post(propertiesUrlWithDoubleSlash, jsonString)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.METHOD_NOT_ALLOWED)
                .fire();
        // no double slash merging -> tries to get double slashed property
        get(propertiesUrlWithDoubleSlash)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(containsCharSequence("Consecutive slashes in JSON pointers are not supported"))
                .fire();

        // no double slash merging -> tries to put double slashed property
        put(propertiesUrlWithDoubleSlashAndId, jsonString)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(containsCharSequence("Consecutive slashes in JSON pointers are not supported"))
                .fire();
        // no double slash merging -> tries to get double slashed property
        get(propertiesUrlWithDoubleSlashAndId)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(containsCharSequence("Consecutive slashes in JSON pointers are not supported"))
                .fire();
        // no double slash merging -> tries to delete double slashed property
        delete(propertiesUrlWithDoubleSlashAndId)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(containsCharSequence("Consecutive slashes in JSON pointers are not supported"))
                .fire();
    }

    @Test
    public void tryToUseDoubleSlashesInUrlForPropertyKey() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "foo";
        final String propertyKey = "sla//shes";
        final String path = ResourcePathBuilder.forThing(thingId).feature(featureId).property(propertyKey).toString();
        final String jsonString = "13";

        put(thingsServiceUrl(TestConstants.API_V_2, path), jsonString)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingBody(containsCharSequence("Consecutive slashes in JSON pointers are not supported"))
                .fire();
    }

}
