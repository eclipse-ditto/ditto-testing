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

import java.util.List;

import javax.annotation.Nullable;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.entity.id.EntityId;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.matcher.PostMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.restassured.response.Response;

/**
 * System test for the metadata feature.
 */
public final class MetadataIT extends IntegrationTest {

    private static final String PUT_METADATA = "put-metadata";
    private static final String GET_METADATA = "get-metadata";
    private static final String DELETE_METADATA = "delete-metadata";
    private static final String DITTO_METADATA = "ditto-metadata";

    private static final JsonFieldSelector FIELD_SELECTOR = JsonFactory.newFieldSelector(Thing.JsonFields.ID,
            Thing.JsonFields.POLICY_ID,
            Thing.JsonFields.ATTRIBUTES,
            Thing.JsonFields.FEATURES,
            Thing.JsonFields.METADATA);

    private static final String LAMP_PROPERTY_COLOR = "color";

    private static final FeatureProperties LAMP_PROPERTIES = FeatureProperties.newBuilder()
            .set(LAMP_PROPERTY_COLOR, JsonObject.newBuilder()
                    .set("r", 100)
                    .set("g", 0)
                    .set("b", 255)
                    .build())
            .build();

    private static final Feature LAMP = Feature.newBuilder()
            .properties(LAMP_PROPERTIES)
            .withId("lamp")
            .build();

    private static final Thing THING_WITH_LAMP = Thing.newBuilder()
            .setFeature(LAMP)
            .build();

    private static final String LAMP_COLOR_R_METADATA_KEY = "/features/lamp/properties/color/r/meta";
    private static final String LAMP_COLOR_G_METADATA_KEY = "/features/lamp/properties/color/g/meta";
    private static final String LAMP_COLOR_B_METADATA_KEY = "/features/lamp/properties/color/b/meta";

    private static final String METADATA_KEY = "/meta";

    private static final String WILDCARD_METADATA_KEY = "*/meta";

    private static final JsonObject METADATA_VALUE = JsonObject.newBuilder()
            .set("issuedAt", "someTimestamp")
            .set("issuedBy", JsonObject.newBuilder()
                    .set("name", "ditto")
                    .set("mail", "ditto@mail.com")
                    .build())
            .build();

    private static final JsonObject EXPECTED_LAMP_METADATA = JsonObject.newBuilder()
            .set("_metadata", JsonObject.newBuilder()
                    .set(LAMP_COLOR_R_METADATA_KEY, METADATA_VALUE)
                    .build())
            .build();

    private static final JsonObject EXPECTED_METADATA_WITHOUT_LAMP_COLOR_METADATA = JsonObject.newBuilder()
            .set("_metadata", JsonObject.newBuilder()
                    .set("/features/lamp/properties", JsonObject.newBuilder().build())
                    .build())
            .build();

    @Test
    public void createThingWithInlineMetadata() {
        final var thingJsonWithMetadata = THING_WITH_LAMP.toJson()
                .toBuilder()
                .set("_metadata", JsonObject.newBuilder()
                        .set(LAMP_COLOR_R_METADATA_KEY, METADATA_VALUE)
                        .build())
                .build();

        final var thingId = postThing(thingJsonWithMetadata);

        getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingBody(contains(EXPECTED_LAMP_METADATA))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putMetadataWithThingCreation() {
        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                .set("key", LAMP_COLOR_R_METADATA_KEY)
                .set("value", METADATA_VALUE)
                .build());

        final EntityId thingId = postThing(THING_WITH_LAMP, metadata);

        getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingBody(contains(EXPECTED_LAMP_METADATA))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putMetadataWithInlineMetadataResultsInError() {
        final var thingJsonWithMetadata = THING_WITH_LAMP.toJson()
                .toBuilder()
                .set("_metadata", JsonObject.newBuilder()
                        .set(LAMP_COLOR_R_METADATA_KEY, METADATA_VALUE)
                        .build())
                .build();

        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                .set("key", LAMP_COLOR_R_METADATA_KEY)
                .set("value", METADATA_VALUE)
                .build());

        postThing(TestConstants.API_V_2, thingJsonWithMetadata)
                .withHeader(PUT_METADATA, metadata.toString())
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("things:metadata.notmodifiable")
                .expectingBody(Matchers.containsString("Multiple occurrences of metadata per request is not allowed."))
                .fire();
    }

    @Test
    public void putMetadataWithIncorrectWildcardResultsInError() {
        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                .set("key", "features/*/properties/*/*/meta")
                .set("value", METADATA_VALUE)
                .build());

        postThing(TestConstants.API_V_2, THING_WITH_LAMP.toJsonString())
                .withHeader(PUT_METADATA, metadata.toString())
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("header.invalid")
                .fire();
    }

    @Test
    public void putMetadataWithThingUpdate() {
        final EntityId thingId = postThing(THING_WITH_LAMP);

        final Response response = getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingBody(containsOnly(
                        List.of(Thing.JsonFields.ID, Thing.JsonFields.POLICY_ID, Thing.JsonFields.FEATURES)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final Thing thing = ThingsModelFactory.newThing(response.body().asString());
        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                .set("key", LAMP_COLOR_R_METADATA_KEY)
                .set("value", METADATA_VALUE)
                .build());

        putThing(thing, metadata);

        getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingBody(contains(EXPECTED_LAMP_METADATA))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putWildcardMetadataWithThingUpdate() {
        final EntityId thingId = postThing(THING_WITH_LAMP);

        final Response response = getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingBody(containsOnly(
                        List.of(Thing.JsonFields.ID, Thing.JsonFields.POLICY_ID, Thing.JsonFields.FEATURES)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        final Thing thing = ThingsModelFactory.newThing(response.body().asString());
        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                .set("key", WILDCARD_METADATA_KEY)
                .set("value", METADATA_VALUE)
                .build());

        putThing(thing, metadata);

        final var expectedMetadata = JsonObject.newBuilder()
                .set("_metadata", JsonObject.newBuilder()
                        .set("/thingId/meta", METADATA_VALUE)
                        .set("/policyId/meta", METADATA_VALUE)
                        .set("/features/lamp/properties/color/r/meta", METADATA_VALUE)
                        .set("/features/lamp/properties/color/g/meta", METADATA_VALUE)
                        .set("/features/lamp/properties/color/b/meta", METADATA_VALUE)
                        .build())
                .build();

        getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingBody(contains(expectedMetadata))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putMetadataWithPropertyUpdate() {
        final EntityId thingId = postThing(THING_WITH_LAMP);

        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                .set("key", METADATA_KEY)
                .set("value", METADATA_VALUE)
                .build());

        putProperty(TestConstants.API_V_2, thingId, LAMP.getId(), "color/r", "42")
                .withHeader(PUT_METADATA, metadata.toString())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingBody(contains(EXPECTED_LAMP_METADATA))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putWildcardMetadataForFeaturesWithThingUpdate() {
        final EntityId thingId = postThing(THING_WITH_LAMP);

        final Response response = getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingBody(containsOnly(
                        List.of(Thing.JsonFields.ID, Thing.JsonFields.POLICY_ID, Thing.JsonFields.FEATURES)))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();


        final Feature feature = Feature.newBuilder()
                .properties(FeatureProperties.newBuilder()
                        .set("featurePurpose", "being amazing :)")
                        .build())
                .withId("ditto-thing")
                .build();

        final Thing thing = ThingsModelFactory.newThing(response.body().asString()).setFeature(feature);
        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                .set("key", "features/*/properties/*/meta")
                .set("value", METADATA_VALUE)
                .build());

        putThing(thing, metadata);

        final var expectedMetadata = JsonObject.newBuilder()
                .set("_metadata", JsonObject.newBuilder()
                        .set("/features/lamp/properties/color/meta", METADATA_VALUE)
                        .set("/features/ditto-thing/properties/featurePurpose/meta", METADATA_VALUE)
                        .build())
                .build();

        getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingBody(contains(expectedMetadata))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putWildcardMetadataWithPropertyUpdate() {
        final EntityId thingId = postThing(THING_WITH_LAMP);

        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                .set("key", "properties/*/meta")
                .set("value", METADATA_VALUE)
                .build());

        putFeature(TestConstants.API_V_2, thingId, LAMP.getId(),
                JsonObject.newBuilder()
                        .set("properties", JsonObject.newBuilder()
                                .set("color", JsonObject.newBuilder()
                                        .set("r", 255)
                                        .set("g", 255)
                                        .set("b", 255)
                                        .build())
                                .build())
                        .build()
                        .toString())
                .withHeader(PUT_METADATA, metadata.toString())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final var expectedMetadata = JsonObject.newBuilder()
                .set("_metadata", JsonObject.newBuilder()
                        .set("/features/lamp/properties/color/meta", METADATA_VALUE)
                        .build())
                .build();

        getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingBody(contains(expectedMetadata))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putMetadataWithPatch() {
        final ThingId thingId = postThing(THING_WITH_LAMP);

        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                .set("key", METADATA_KEY)
                .set("value", METADATA_VALUE)
                .build());

        patchThing(thingId, JsonPointer.of("/features/lamp/properties/color/r"), JsonValue.of("42"))
                .withHeader(PUT_METADATA, metadata.toString())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingBody(contains(EXPECTED_LAMP_METADATA))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void putWildcardMetadataWithPatch() {
        final ThingId thingId = postThing(THING_WITH_LAMP);

        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                .set("key", "properties/*/meta")
                .set("value", METADATA_VALUE)
                .build());

        patchThing(thingId, JsonPointer.of("/features/lamp/"),
                JsonObject.newBuilder()
                        .set("properties", JsonObject.newBuilder()
                                .set("color", JsonObject.newBuilder()
                                        .set("r", 255)
                                        .set("g", 255)
                                        .set("b", 255)
                                        .build())
                                .build())
                        .build())
                .withHeader(PUT_METADATA, metadata.toString())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final var expectedMetadata = JsonObject.newBuilder()
                .set("_metadata", JsonObject.newBuilder()
                        .set("/features/lamp/properties/color/meta", METADATA_VALUE)
                        .build())
                .build();

        getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingBody(contains(expectedMetadata))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void getMetadata() {
        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                        .set("key", LAMP_COLOR_R_METADATA_KEY)
                        .set("value", METADATA_VALUE)
                        .build(),
                JsonObject.newBuilder()
                        .set("key", LAMP_COLOR_G_METADATA_KEY)
                        .set("value", METADATA_VALUE)
                        .build(),
                JsonObject.newBuilder()
                        .set("key", LAMP_COLOR_B_METADATA_KEY)
                        .set("value", METADATA_VALUE)
                        .build());

        final EntityId thingId = postThing(THING_WITH_LAMP, metadata);

        final var expectedMetadata = JsonObject.newBuilder()
                .set(LAMP_COLOR_R_METADATA_KEY, METADATA_VALUE)
                .build();

        getThing(TestConstants.API_V_2, thingId)
                .withHeader(GET_METADATA, LAMP_COLOR_R_METADATA_KEY)
                .expectingHttpStatus(HttpStatus.OK)
                .expectingHeader(DITTO_METADATA, Matchers.equalTo(expectedMetadata.toString()))
                .fire();
    }

    @Test
    public void getWildcardMetadata() {
        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                .set("key", "/features/lamp/properties/*/meta")
                .set("value", METADATA_VALUE)
                .build());

        final EntityId thingId = postThing(THING_WITH_LAMP, metadata);

        final var expectedMetadata = JsonObject.newBuilder()
                .set("/features/lamp/properties/color/meta", METADATA_VALUE)
                .build();

        getThing(TestConstants.API_V_2, thingId)
                .withHeader(GET_METADATA, "/features/lamp/properties/*/meta")
                .expectingHttpStatus(HttpStatus.OK)
                .expectingHeader(DITTO_METADATA, Matchers.equalTo(expectedMetadata.toString()))
                .fire();
    }

    @Test
    public void getNestedWildcardMetadata() {
        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                        .set("key", LAMP_COLOR_R_METADATA_KEY)
                        .set("value", METADATA_VALUE)
                        .build(),
                JsonObject.newBuilder()
                        .set("key", LAMP_COLOR_G_METADATA_KEY)
                        .set("value", METADATA_VALUE)
                        .build(),
                JsonObject.newBuilder()
                        .set("key", LAMP_COLOR_B_METADATA_KEY)
                        .set("value", METADATA_VALUE)
                        .build());

        final EntityId thingId = postThing(THING_WITH_LAMP, metadata);

        final var expectedMetadata = JsonObject.newBuilder()
                .set("/features/lamp/properties/color/r/meta", METADATA_VALUE)
                .build();

        getThing(TestConstants.API_V_2, thingId)
                .withHeader(GET_METADATA, "/features/lamp/properties/*/r/meta")
                .expectingHttpStatus(HttpStatus.OK)
                .expectingHeader(DITTO_METADATA, Matchers.equalTo(expectedMetadata.toString()))
                .fire();
    }

    @Test
    public void multipleMetadataHeaderResultInError() {
        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                .set("key", LAMP_COLOR_R_METADATA_KEY)
                .set("value", METADATA_VALUE)
                .build());

        final EntityId thingId = postThing(THING_WITH_LAMP, metadata);

        getThing(TestConstants.API_V_2, thingId)
                .withHeader(GET_METADATA, LAMP_COLOR_R_METADATA_KEY)
                .withHeader(DELETE_METADATA, LAMP_COLOR_G_METADATA_KEY)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("things:metadata.headersconflict")
                .expectingBody(Matchers.containsString("Multiple metadata headers were specified."))
                .fire();
    }

    @Test
    public void deleteMetadata() {
        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                        .set("key", LAMP_COLOR_R_METADATA_KEY)
                        .set("value", METADATA_VALUE)
                        .build(),
                JsonObject.newBuilder()
                        .set("key", LAMP_COLOR_G_METADATA_KEY)
                        .set("value", METADATA_VALUE)
                        .build(),
                JsonObject.newBuilder()
                        .set("key", LAMP_COLOR_B_METADATA_KEY)
                        .set("value", METADATA_VALUE)
                        .build());

        final ThingId thingId = postThing(THING_WITH_LAMP, metadata);

        final var expectedMetadata = JsonObject.newBuilder()
                .set(LAMP_COLOR_B_METADATA_KEY, METADATA_VALUE)
                .set(LAMP_COLOR_G_METADATA_KEY, METADATA_VALUE)
                .set(LAMP_COLOR_R_METADATA_KEY, METADATA_VALUE)
                .build();

        getThing(TestConstants.API_V_2, thingId)
                .withHeader(GET_METADATA, "/features/lamp/properties/color")
                .expectingHttpStatus(HttpStatus.OK)
                .expectingHeader(DITTO_METADATA, Matchers.equalTo(expectedMetadata.toString()))
                .fire();

        patchThing(thingId, JsonPointer.of("/features/lamp/properties/color/r"), JsonValue.of("42"))
                .withHeader(DELETE_METADATA, "meta")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final var expectedMetadataAfterDeletion = JsonObject.newBuilder()
                .set(LAMP_COLOR_B_METADATA_KEY, METADATA_VALUE)
                .set(LAMP_COLOR_G_METADATA_KEY, METADATA_VALUE)
                .set("/features/lamp/properties/color/r", JsonObject.empty())
                .build();

        getThing(TestConstants.API_V_2, thingId)
                .withHeader(GET_METADATA, "/features/lamp/properties/color")
                .expectingHttpStatus(HttpStatus.OK)
                .expectingHeader(DITTO_METADATA, Matchers.equalTo(expectedMetadataAfterDeletion.toString()))
                .fire();
    }

    @Test
    public void deleteMetadataWhenPropertyIsDeleted() {
        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                .set("key", LAMP_COLOR_R_METADATA_KEY)
                .set("value", METADATA_VALUE)
                .build());

        final EntityId thingId = postThing(THING_WITH_LAMP, metadata);

        getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingBody(contains(EXPECTED_LAMP_METADATA))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        deleteProperty(TestConstants.API_V_2, thingId, LAMP.getId(), LAMP_PROPERTY_COLOR)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final Response response = getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
        final JsonObject jsonResponse = JsonObject.of(response.body().asString());
        final JsonObject actualMetadata = jsonResponse.get(Thing.JsonFields.METADATA);

        assertThat(actualMetadata).isEqualTo(EXPECTED_METADATA_WITHOUT_LAMP_COLOR_METADATA);
    }

    @Test
    public void deleteMetadataWhenPropertiesAreOverwrittenWithEmptyObject() {
        final JsonArray metadata = JsonArray.of(JsonObject.newBuilder()
                .set("key", LAMP_COLOR_R_METADATA_KEY)
                .set("value", METADATA_VALUE)
                .build());

        final EntityId thingId = postThing(THING_WITH_LAMP, metadata);

        getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingBody(contains(EXPECTED_LAMP_METADATA))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();

        putProperties(TestConstants.API_V_2, thingId, LAMP.getId(), JsonObject.empty().toString())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final Response response = getThing(TestConstants.API_V_2, thingId)
                .withFields(FIELD_SELECTOR)
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
        final JsonObject jsonResponse = JsonObject.of(response.body().asString());
        final JsonObject actualMetadata = jsonResponse.get(Thing.JsonFields.METADATA);

        assertThat(actualMetadata).isEqualTo(EXPECTED_METADATA_WITHOUT_LAMP_COLOR_METADATA);
    }

    private static ThingId postThing(final Thing thing) {
        return postThing(thing, null);
    }

    private static ThingId postThing(final Thing thing, @Nullable final JsonArray metadata) {
        return postThing(thing.toJson(), metadata);
    }

    private static ThingId postThing(final JsonObject thingJson) {
        return postThing(thingJson, null);
    }

    private static ThingId postThing(final JsonObject thingJson, @Nullable final JsonArray metadata) {
        final PostMatcher matcher = postThing(TestConstants.API_V_2, thingJson);
        if (null != metadata) {
            matcher.withHeader(PUT_METADATA, metadata.toString());
        }
        final Response response = matcher
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire();
        return ThingsModelFactory.newThing(response.body()
                        .asString())
                .getEntityId()
                .orElseThrow(() -> new IllegalStateException("Thing does not have an ID!"));
    }

    private static void putThing(final Thing thing, @Nullable final JsonArray metadata) {
        final PutMatcher matcher = putThing(TestConstants.API_V_2, thing.toJson(), JsonSchemaVersion.V_2);
        if (null != metadata) {
            matcher.withHeader(PUT_METADATA, metadata.toString());
        }
        matcher.expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

}
