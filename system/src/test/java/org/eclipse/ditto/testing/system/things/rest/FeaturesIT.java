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

import static org.assertj.core.api.Assertions.assertThat;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.testing.common.HttpParameter;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ResourcePathBuilder;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.UriEncoding;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureDefinition;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Features;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration Tests for /things/features resources.
 */
public final class FeaturesIT extends IntegrationTest {

    private static final String SPECIAL_FEATURE_ID = "!\\\"#$%&'()*+,:;=?@[]{|} aZ0";

    @Test
    public void putAndRetrieveFeatures() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featuresJson = "{\"test\":{\"properties\":{\"foo\":\"bar\"}}}";

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getFeatures(TestConstants.API_V_2, thingId)
                .expectingBody(contains(JsonFactory.readFrom(featuresJson)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putAndRetrieveFeaturesWithDefinition() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final Features features = ThingsModelFactory.newFeatures(Feature.newBuilder()
                .properties(ThingsModelFactory.newFeaturePropertiesBuilder()
                        .set("foo", "bar")
                        .build())
                .definition(FeatureDefinition.fromIdentifier("foo:bar:1"))
                .withId("test")
                .build());
        final JsonObject featuresJson = features.toJson();

        putFeatures(TestConstants.API_V_2, thingId, featuresJson)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getFeatures(TestConstants.API_V_2, thingId)
                .expectingBody(contains(featuresJson))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putNullFeaturesAndRetrieveThem() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.nullLiteral())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        getFeatures(TestConstants.API_V_2, thingId)
                .expectingBody(contains(JsonFactory.nullLiteral()))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putEmptyJsonFeaturesAndRetrieveThem() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.readFrom("{}"))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getFeatures(TestConstants.API_V_2, thingId)
                .expectingBody(contains(JsonFactory.readFrom("{}")))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putNewFeatureAndRetrieveIt() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);
        final String featureId = "test";
        final String featureJson = "{\"properties\":{\"foo\":\"bar\"}}";

        final String featureLocation =
                putFeature(TestConstants.API_V_2, thingId, featureId, JsonFactory.newObject(featureJson))
                        .expectingBody(contains(JsonFactory.newObject(featureJson)))
                        .expectingHttpStatus(HttpStatus.CREATED)
                        .fire()
                        .getHeader("Location");

        getByLocation(featureLocation)
                .expectingBody(contains(JsonFactory.readFrom(featureJson)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        getFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(contains(JsonFactory.readFrom(featureJson)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putNewFeatureAndRetrieveItWithWildcardFieldSelector() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);
        final String featureId = "test";
        final String featureJson = "{\"properties\":{\"foo\":\"bar\", \"bar\":\"foo\"}}";

        for (int i = 0; i < 3; i++) {
            putFeature(TestConstants.API_V_2, thingId, featureId + i, JsonFactory.newObject(featureJson))
                    .expectingBody(contains(JsonFactory.newObject(featureJson)))
                    .expectingHttpStatus(HttpStatus.CREATED)
                    .fire()
                    .getHeader("Location");
        }

        final String expectedFeatureJson = "{\"properties\":{\"foo\":\"bar\"}}";
        getFeatures(TestConstants.API_V_2, thingId)
                .withParam(HttpParameter.FIELDS, "*/properties/foo")
                .expectingBody(contains(JsonObject.newBuilder()
                        .set("test0", JsonFactory.readFrom(expectedFeatureJson))
                        .set("test1", JsonFactory.readFrom(expectedFeatureJson))
                        .set("test2", JsonFactory.readFrom(expectedFeatureJson))
                        .build()))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putExistingFeatureAndRetrieveId() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "test";

        final String featureJson = "{\"properties\":{\"foo\":\"bar\"}}";
        putFeature(TestConstants.API_V_2, thingId, featureId, JsonFactory.newObject(featureJson))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final String updatedFeatureJson = "{\"properties\":{\"foo\":\"bar\",\"foo1\":\"bar1\"}}";
        putFeature(TestConstants.API_V_2, thingId, featureId, JsonFactory.newObject(updatedFeatureJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(contains(JsonFactory.readFrom(updatedFeatureJson)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putFeatureWithNullValueAndRetrieveIt() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "test";

        putFeature(TestConstants.API_V_2, thingId, featureId, JsonFactory.newObject("null"))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(contains(JsonFactory.readFrom("null")))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final String updateFeatureJson = "{\"properties\":{\"foo\":\"bar\"}}";
        putFeature(TestConstants.API_V_2, thingId, featureId, JsonFactory.newObject(updateFeatureJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(contains(JsonFactory.readFrom(updateFeatureJson)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putFeatureWithEmptyValueAndRetrieveIt() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "test";

        putFeature(TestConstants.API_V_2, thingId, featureId, JsonFactory.newObject("{}"))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(contains(JsonFactory.readFrom("{}")))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putFeatureWithNullPropertiesAndRetrieveIt() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "test";

        final String featureJson = "{\"properties\":null}";
        putFeature(TestConstants.API_V_2, thingId, featureId, JsonFactory.newObject(featureJson))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(contains(JsonFactory.readFrom(featureJson)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putFeatureWithEmptyPropertiesAndRetrieveIt() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "test";

        final String featureJson = "{\"properties\":{}}";
        putFeature(TestConstants.API_V_2, thingId, featureId, JsonFactory.newObject(featureJson))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(contains(JsonFactory.readFrom(featureJson)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putFeatureWithNullDesiredPropertiesAndRetrieveIt() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "test";

        final String featureJson = "{\"desiredProperties\":null}";
        putFeature(TestConstants.API_V_2, thingId, featureId, JsonFactory.newObject(featureJson))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(contains(JsonFactory.readFrom(featureJson)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putFeatureWithEmptyDesiredPropertiesAndRetrieveIt() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "test";

        final String featureJson = "{\"desiredProperties\":{}}";
        putFeature(TestConstants.API_V_2, thingId, featureId, JsonFactory.newObject(featureJson))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(contains(JsonFactory.readFrom(featureJson)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putFeatureOnNullFeaturesAndRetrieveThem() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "test";

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.nullLiteral())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getFeatures(TestConstants.API_V_2, thingId)
                .expectingBody(contains(JsonFactory.nullLiteral()))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final String updateFeatureJson = "{\"properties\":{\"foo\":\"bar\"}}";
        putFeature(TestConstants.API_V_2, thingId, featureId, JsonFactory.newObject(updateFeatureJson))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(contains(JsonFactory.readFrom(updateFeatureJson)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final String expectedFeaturesJson = "{\"" + featureId + "\":{\"properties\":{\"foo\":\"bar\"}}}";
        getFeatures(TestConstants.API_V_2, thingId)
                .expectingBody(contains(JsonFactory.readFrom(expectedFeaturesJson)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void crudFeatureWithSpecialChars() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final String featureId = SPECIAL_FEATURE_ID;
        final JsonObject featureJson = JsonFactory.newObject("{\"properties\":{\"foo\":\"bar\"}}");
        final Feature feature = ThingsModelFactory.newFeatureBuilder(featureJson).useId(featureId).build();
        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId).setFeature(feature).build();

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final String encodedFeatureId = UriEncoding.encodePathSegment(featureId);

        getFeature(TestConstants.API_V_2, thingId, encodedFeatureId)
                .expectingBody(contains(featureJson))
                .expectingHttpStatus(HttpStatus.OK)
                .disableUrlEncoding()
                .fire();

        deleteFeature(TestConstants.API_V_2, thingId, encodedFeatureId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .disableUrlEncoding()
                .fire();

        final String featureLocation = putFeature(TestConstants.API_V_2, thingId, encodedFeatureId, featureJson)
                .expectingHttpStatus(HttpStatus.CREATED)
                .disableUrlEncoding()
                .fire()
                .getHeader("Location");

        assertThat(featureLocation).endsWith(encodedFeatureId);

        getByLocation(featureLocation)
                .expectingBody(contains(featureJson))
                .expectingHttpStatus(HttpStatus.OK)
                .disableUrlEncoding()
                .fire();
    }

    @Test
    public void deleteFeature() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "test";

        final String featureJson = "{\"properties\":{\"foo\":\"bar\"}}";
        putFeature(TestConstants.API_V_2, thingId, featureId, JsonFactory.newObject(featureJson))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        deleteFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void deleteFeatureFromNullFeatures() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "unknownFeatureId";

        putFeatures(TestConstants.API_V_2, thingId, "null")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getFeatures(TestConstants.API_V_2, thingId)
                .expectingBody(contains(JsonFactory.nullLiteral()))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void deleteFeatures() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        deleteFeatures(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getFeatures(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        deleteFeatures(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .expectingBody(containsOnlyJsonKeysAtTopLevel("thingId", "policyId", "attributes"))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void deleteEmptyFeatures() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.readFrom("{}"))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getFeatures(TestConstants.API_V_2, thingId)
                .expectingBody(contains(JsonFactory.readFrom("{}")))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteFeatures(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getFeatures(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }

    @Test
    public void deleteFeaturesAndPutANewFeature() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "test";

        deleteFeatures(TestConstants.API_V_2, thingId).expectingHttpStatus(HttpStatus.NO_CONTENT).fire();

        final String featureJson = "{\"properties\":{\"foo\":\"bar\"}}";
        putFeature(TestConstants.API_V_2, thingId, featureId, JsonFactory.newObject(featureJson))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        final String expectedFeaturesJson = "{\"" + featureId + "\":{\"properties\":{\"foo\":\"bar\"}}}";
        getFeatures(TestConstants.API_V_2, thingId)
                .expectingBody(contains(JsonFactory.readFrom(expectedFeaturesJson)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteFeatures(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void deleteFeaturesAndPutFeatures() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "test";

        deleteFeatures(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final String featuresJson = "{\"" + featureId + "\":{\"properties\":{\"foo\":\"bar\"}}}";

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingBody(contains(JsonFactory.readFrom(featuresJson)))
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        putFeatures(TestConstants.API_V_2, thingId, JsonFactory.newObject(featuresJson))
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void deleteEveryFeatureGetFeaturesReturnsEmpty() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        deleteFeature(TestConstants.API_V_2, thingId, "Vehicle")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        deleteFeature(TestConstants.API_V_2, thingId, "EnvironmentScanner")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getFeatures(TestConstants.API_V_2, thingId)
                .expectingBody(contains(JsonFactory.readFrom("{}")))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putFeatureWithInvalidJsonStringReturnsBadFormat() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "test";

        final String featureJson = "{\"properties\":{foo:\"bar\"}}";
        putFeature(TestConstants.API_V_2, thingId, featureId, featureJson)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    @Test
    public void putFeatureWithWrongFormatReturnsBadFormat() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureId = "test";

        final String featureJson = "wrongFormatForFeature";
        putFeature(TestConstants.API_V_2, thingId, featureId, featureJson)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    @Test
    public void postFeaturesReturnsMethodNotAllowed() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureJson = "foo";
        postFeatures(TestConstants.API_V_2, thingId, featureJson)
                .expectingHttpStatus(HttpStatus.METHOD_NOT_ALLOWED)
                .fire();
    }

    @Test
    public void putFeaturesWithWrongFormatReturnsBadFormat() {
        final String location = postThing(TestConstants.API_V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .header("Location");

        final String thingId = parseIdFromLocation(location);

        final String featureJson = "wrongFormatForFeature";
        putFeatures(TestConstants.API_V_2, thingId, featureJson)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .fire();
    }

    @Test
    @Category(Acceptance.class)
    public void crudFeatureWithDefinition() {
        final ThingId thingId = ThingId.of(idGenerator().withRandomName());
        final String featureId = "feature-with-definition";
        final FeatureDefinition definition = FeatureDefinition.fromIdentifier("foo:bar:1.0");
        final FeatureProperties initialProperties = FeatureProperties.newBuilder().set("foo", "bar").build();
        final Feature feature = ThingsModelFactory.newFeature(featureId, definition, initialProperties);
        final Thing thing = ThingsModelFactory.newThingBuilder().setId(thingId).setFeature(feature).build();
        final FeatureProperties newProperties = FeatureProperties.newBuilder().set("test", 42).build();
        final Feature newFeatureWithoutDefinition = feature.removeDefinition().setProperties(newProperties);

        putThing(TestConstants.API_V_2, thing, JsonSchemaVersion.V_2)
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();

        getFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(contains(feature.toJson()))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        putProperties(TestConstants.API_V_2, thingId, featureId, newProperties.toJsonString())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getDefinition(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(contains(definition.toJson()))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteDefinition(TestConstants.API_V_2, thingId, featureId)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getFeature(TestConstants.API_V_2, thingId, featureId)
                .expectingBody(contains(newFeatureWithoutDefinition.toJson()))
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

        final String path = ResourcePathBuilder.forThing(thingId).features().toString();
        final String jsonString = "{}";
        final String featuresUrlWithDoubleSlash = dittoUrl(TestConstants.API_V_2, path) + "//";
        final String featuresUrlWithDoubleSlashAndId =
                featuresUrlWithDoubleSlash + "foo";

        // no double slash merging -> tries to post against empty feature id
        post(featuresUrlWithDoubleSlash, jsonString)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
        // no double slash merging -> tries to get empty feature id
        get(featuresUrlWithDoubleSlash)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();

        // no double slash merging -> tries to put empty feature id
        put(featuresUrlWithDoubleSlashAndId, jsonString)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
        // no double slash merging -> tries to get empty feature id
        get(featuresUrlWithDoubleSlashAndId)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
        // no double slash merging -> tries to delete empty feature id
        delete(featuresUrlWithDoubleSlashAndId)
                .disableUrlEncoding()
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .fire();
    }
}
