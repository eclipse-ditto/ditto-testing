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

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.function.UnaryOperator;

import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.json.JsonValue;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.Resources;
import org.eclipse.ditto.testing.common.HttpHeader;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.ThingJsonProducer;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.things.model.Attributes;
import org.eclipse.ditto.things.model.AttributesModelFactory;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.FeatureDefinition;
import org.eclipse.ditto.things.model.FeatureProperties;
import org.eclipse.ditto.things.model.Features;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingDefinition;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for merge functionality using the HTTP API.
 * <p>
 * Test merging at the following resources:
 * <ul>
 * <li>thingId</li>
 * <li>policyId</li>
 * <li>definition</li>
 * <li>attributes</li>
 * <li>attributes/attributePath</li>
 * <li>features</li>
 * <li>features/featureId</li>
 * <li>features/featureId/definition</li>
 * <li>features/featureId/properties</li>
 * <li>features/featureId/properties/propertyPath</li>
 * <li>features/featureId/desiredProperties</li>
 * <li>features/featureId/desiredProperties/propertyPath</li>
 * </ul>
 * <p>
 * The merge is also applied
 * <ul>
 *     <li>
 *         at root level, which should have the same result (e.g. with path {@code /} and value {@code { "attributes" : { "attributePath" : 1234 } }})
 *     </li>
 *     <li>with {@code null} value, which deletes the entry at the given path</li>
 *     <li>with an invalid value, which results in an error</li>
 * </ul>
 */
public class MergeIT extends IntegrationTest {

    @Test
    public void mergeNonExistentThing() {
        patchThing(ThingId.of(serviceEnv.getDefaultNamespaceName(), Long.toHexString(System.currentTimeMillis())),
                Thing.JsonFields.ATTRIBUTES.getPointer(), JsonObject.empty())
                .expectingHttpStatus(HttpStatus.NOT_FOUND)
                .expectingErrorCode("things:thing.notfound")
                .fire();
    }

    @Test
    public void mergeWithInvalidContentType() {
        patchThing(TestConstants.API_V_2, TestConstants.CONTENT_TYPE_APPLICATION_JSON, ThingId.generateRandom("org.eclipse.ditto"),
                Thing.JsonFields.ATTRIBUTES.getPointer(), JsonObject.empty())
                .expectingHttpStatus(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
                .expectingErrorCode("mediatype.unsupported")
                .fire();
    }

    @Test
    public void mergeWithNullThing() {
        patchThing(TestConstants.API_V_2, ThingId.generateRandom("org.eclipse.ditto"), JsonPointer.empty(), JsonFactory.nullLiteral())
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("things:merge.invalid")
                .fire();
    }

    @Test
    public void mergeEmptyThing() {
        patchThing(TestConstants.API_V_2, ThingId.generateRandom("org.eclipse.ditto"), JsonPointer.empty(), JsonObject.empty())
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("things:merge.invalid")
                .fire();
    }

    @Test
    public void mergeThingId() {
        final Thing thing = newThing();
        final ThingId thingId = thing.getEntityId().orElseThrow();
        final JsonObject mergePatch = JsonObject.newBuilder().set(Thing.JsonFields.ID, thingId.toString()).build();
        patchThing(thingId, JsonPointer.empty(), mergePatch)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }

    @Test
    public void mergeWithNullThingId() {
        final Thing thing = newThing();
        final ThingId thingId = thing.getEntityId().orElseThrow();
        final JsonObject mergePatch = JsonObject.newBuilder().set(Thing.JsonFields.ID, null).build();
        patchThing(thingId, JsonPointer.empty(), mergePatch)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("things:id.notdeletable")
                .fire();
    }

    @Test
    public void mergeThingIdWithDifferentThingId() {
        final Thing thing = newThing();
        final JsonObject mergePatch = JsonObject.newBuilder()
                .set(Thing.JsonFields.ID,
                        ThingId.of(serviceEnv.getDefaultNamespaceName(), Long.toHexString(System.currentTimeMillis()))
                                .toString())
                .build();
        patchThing(thing.getEntityId().orElseThrow(), JsonPointer.empty(), mergePatch)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode("things:id.notsettable")
                .fire();
    }

    @Test
    public void mergePolicyId() {
        final Thing thing = newThing();
        final ThingId thingId = thing.getEntityId().orElseThrow();
        final Policy policy = PoliciesModelFactory.newPolicy(
                getPolicy(thing.getPolicyId().orElseThrow()).expectingHttpStatus(HttpStatus.OK)
                        .fire()
                        .body()
                        .asString());

        final PolicyId policyId =
                PolicyId.of(serviceEnv.getDefaultNamespaceName(), Long.toHexString(System.currentTimeMillis()));
        putPolicy(policy.toBuilder().setId(policyId).build()).expectingHttpStatus(HttpStatus.CREATED).fire();

        final JsonObject policyIdPatch = JsonObject.newBuilder()
                .set(Thing.JsonFields.POLICY_ID, policyId.toString())
                .build();

        patchThing(thingId, JsonPointer.empty(), policyIdPatch)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        final Thing patchedThing = getPatchedThing(thingId);

        assertThat(patchedThing.getPolicyId()).contains(policyId);
    }

    @Test
    public void mergeWithInvalidPolicyId() {
        mergeInvalid(Thing.JsonFields.POLICY_ID.getPointer(), JsonValue.of("invalid"), "policies:id.invalid");
    }

    @Test
    public void mergeWithNullPolicyId() {
        mergeInvalid(Thing.JsonFields.POLICY_ID.getPointer(), JsonFactory.nullLiteral(),
                "things:policyId.notdeletable");
    }

    @Test
    public void mergeThingDefinition() {
        final ThingDefinition thingDefinition = ThingsModelFactory.newDefinition("thing:definition:1.0.1");
        mergeSuccessful(Thing.JsonFields.DEFINITION.getPointer(),
                thingDefinition.toJson(),
                t -> t.setDefinition(thingDefinition));
    }

    @Test
    public void mergeNullThingDefinition() {
        mergeNull(Thing.JsonFields.DEFINITION.getPointer());
    }

    @Test
    public void mergeWithInvalidThingDefinitionAndExpectError() {
        mergeInvalid(Thing.JsonFields.DEFINITION.getPointer(), JsonValue.of("things:definition.identifier.invalid"),
                "things:definition.identifier.invalid");
    }

    @Test
    @Category(Acceptance.class)
    public void mergeAttributes() {
        final JsonObject attributesJson = JsonObject.newBuilder()
                .set("some", "attribute")
                .set("nested", JsonObject.newBuilder().set("number", 1234).build())
                .set("boolean", false)
                .build();

        mergeSuccessful(Thing.JsonFields.ATTRIBUTES.getPointer(), attributesJson,
                thing -> {
                    final JsonObject mergedAttributesJson = JsonFactory.mergeJsonValues(attributesJson,
                            thing.getAttributes().orElseThrow()).asObject();
                    final Attributes mergedAttributes = AttributesModelFactory.newAttributes(mergedAttributesJson);
                    return thing.setAttributes(mergedAttributes);
                });
    }

    @Test
    public void mergeNullAttributes() {
        mergeNull(Thing.JsonFields.ATTRIBUTES.getPointer());
    }

    @Test
    public void mergeInvalidAttributesAndExpectError() {
        mergeInvalid(Thing.JsonFields.ATTRIBUTES.getPointer(), JsonValue.of(Math.PI), "json.invalid");
    }

    @Test
    public void mergeAttribute() {
        final JsonObject attributeJson = JsonObject.newBuilder()
                .set("some", "attribute")
                .set("nested", JsonObject.newBuilder().set("number", 1234).build())
                .set("boolean", false)
                .build();

        final JsonPointer attributePath = JsonPointer.of("merge");
        mergeSuccessful(Thing.JsonFields.ATTRIBUTES.getPointer().append(attributePath),
                attributeJson,
                thing -> thing.setAttribute(attributePath, attributeJson));
    }

    @Test
    public void mergeNullAttribute() {
        mergeNull(Thing.JsonFields.ATTRIBUTES.getPointer().append(JsonPointer.of("VIN")));
    }

    @Test
    @Category(Acceptance.class)
    public void mergeFeatures() {
        final JsonObject featuresJson = JsonObject.newBuilder()
                .set("feature1", JsonObject.empty())
                .set("feature2", JsonObject.newBuilder()
                        .set(Feature.JsonFields.DEFINITION, ThingsModelFactory.newFeatureDefinition(
                                JsonArray.of("definition1:feature2:1.2.3", "definition2:feature2:2.1.0")).toJson())
                        .build())
                .build();

        mergeSuccessful(Thing.JsonFields.FEATURES.getPointer(),
                featuresJson,
                thing -> {
                    final JsonObject mergedFeaturesJson = JsonFactory.mergeJsonValues(featuresJson,
                            thing.getFeatures().orElseThrow().toJson()).asObject();
                    final Features mergedFeatures = ThingsModelFactory.newFeatures(mergedFeaturesJson);
                    return thing.setFeatures(mergedFeatures);
                });
    }

    @Test
    public void mergeNullFeatures() {
        mergeNull(Thing.JsonFields.FEATURES.getPointer());
    }

    @Test
    public void mergeInvalidFeaturesAndExpectError() {
        mergeInvalid(Thing.JsonFields.FEATURES.getPointer(), JsonValue.of(Math.PI), "json.invalid");
    }

    @Test
    public void mergeFeature() {
        final JsonObject featureJson = JsonObject.newBuilder()
                .set(Feature.JsonFields.DEFINITION, ThingsModelFactory.newFeatureDefinition(
                        JsonArray.of("definition1:feature2:1.2.3", "definition2:feature2:2.1.0")).toJson())
                .set(Feature.JsonFields.PROPERTIES, JsonObject.newBuilder()
                        .set("number", Math.E)
                        .set("nested", JsonObject.newBuilder().set("string", UUID.randomUUID().toString()).build())
                        .set("boolean", false)
                        .build())
                .build();

        final String featureId = "Vehicle";
        mergeSuccessful(Thing.JsonFields.FEATURES.getPointer().append(JsonPointer.of(featureId)),
                featureJson,
                thing -> {
                    final JsonObject jsonObject =
                            thing.getFeatures().flatMap(f -> f.getFeature(featureId)).orElseThrow().toJson();
                    final JsonObject mergedFeatureJson =
                            JsonFactory.mergeJsonValues(featureJson, jsonObject).asObject();
                    final Feature mergedFeature =
                            ThingsModelFactory.newFeatureBuilder(mergedFeatureJson).useId(featureId).build();
                    return thing.setFeature(mergedFeature);
                });
    }

    @Test
    public void mergeNullFeature() {
        mergeNull(Thing.JsonFields.FEATURES.getPointer().append(JsonPointer.of("Vehicle")));
    }

    @Test
    public void mergeInvalidFeatureAndExpectError() {
        mergeInvalid(Thing.JsonFields.FEATURES.getPointer().append(JsonPointer.of("Vehicle")), JsonValue.of(Math.PI),
                "json.invalid");
    }

    @Test
    public void mergeFeatureDefinition() {
        final FeatureDefinition definitionIdentifiers = ThingsModelFactory.newFeatureDefinition(
                JsonArray.of("new:definition:4.5.6", "definition2:feature2:2.1.0"));
        mergeSuccessful(getFeaturePath("feature2", Feature.JsonFields.DEFINITION.getPointer()),
                definitionIdentifiers.toJson(),
                thing -> thing.setFeatureDefinition("feature2", definitionIdentifiers));
    }

    @Test
    public void mergeNullFeatureDefinition() {
        mergeNull(getFeaturePath("Vehicle", Feature.JsonFields.DEFINITION.getPointer()));
    }

    @Test
    public void mergeInvalidFeatureDefinitionAndExpectError() {
        mergeInvalid(getFeaturePath("feature2", Feature.JsonFields.DEFINITION.getPointer()),
                JsonValue.of(Math.PI), "json.invalid");
    }

    @Test
    public void mergeFeatureProperties() {
        final String featureId = "Vehicle";
        final JsonObject propertiesJson = JsonObject.newBuilder()
                .set("number", Math.E)
                .set("nested", JsonObject.newBuilder().set("string", UUID.randomUUID().toString()).build())
                .set("boolean", false)
                .build();

        mergeSuccessful(getFeaturePath(featureId, Feature.JsonFields.PROPERTIES.getPointer()),
                propertiesJson,
                thing -> {
                    final FeatureProperties mergedProperties = thing.getFeatures()
                            .flatMap(features -> features.getFeature(featureId))
                            .flatMap(Feature::getProperties)
                            .map(FeatureProperties::toJson)
                            .map(existing -> JsonFactory.mergeJsonValues(propertiesJson, existing))
                            .map(j -> ThingsModelFactory.newFeatureProperties(j.asObject()))
                            .orElseThrow();
                    return thing.setFeatureProperties(featureId, mergedProperties);
                });
    }

    @Test
    public void mergeNullFeatureProperties() {
        mergeNull(getFeaturePath("Vehicle", Feature.JsonFields.PROPERTIES.getPointer()));
    }

    @Test
    public void mergeInvalidFeaturePropertiesAndExpectError() {
        mergeInvalid(getFeaturePath("Vehicle", Feature.JsonFields.PROPERTIES.getPointer()),
                JsonValue.of(Math.PI), "json.invalid");
    }

    @Test
    public void mergeFeatureProperty() {
        final JsonPointer propertyPath = JsonPointer.of("mergedProperty");
        final JsonValue propertyValue = JsonValue.of(42);
        mergeSuccessful(
                Thing.JsonFields.FEATURES.getPointer()
                        .append(JsonPointer.of("Vehicle"))
                        .append(Feature.JsonFields.PROPERTIES.getPointer())
                        .append(propertyPath),
                propertyValue,
                thing -> thing.setFeatureProperty("Vehicle", propertyPath, propertyValue));
    }

    @Test
    public void mergeNullFeatureProperty() {
        mergeNull(Thing.JsonFields.FEATURES.getPointer()
                .append(JsonPointer.of("Vehicle"))
                .append(Feature.JsonFields.PROPERTIES.getPointer())
                .append(JsonPointer.of("status/speed")));
    }

    @Test
    public void mergeDesiredFeatureProperties() {
        final String featureId = "Vehicle";
        final JsonObject propertiesJson = JsonObject.newBuilder()
                .set("number", Math.E)
                .set("nested", JsonObject.newBuilder().set("string", UUID.randomUUID().toString()).build())
                .set("boolean", false)
                .build();

        mergeSuccessful(getFeaturePath(featureId, Feature.JsonFields.DESIRED_PROPERTIES.getPointer()),
                propertiesJson,
                thing -> {
                    final FeatureProperties mergedProperties = thing.getFeatures()
                            .flatMap(features -> features.getFeature(featureId))
                            .map(f -> f.getDesiredProperties().orElse(ThingsModelFactory.emptyFeatureProperties()))
                            .map(FeatureProperties::toJson)
                            .map(existing -> JsonFactory.mergeJsonValues(propertiesJson, existing))
                            .map(j -> ThingsModelFactory.newFeatureProperties(j.asObject()))
                            .orElseThrow();
                    return thing.setFeatureDesiredProperties(featureId, mergedProperties);
                });
    }

    @Test
    public void mergeNullDesiredFeatureProperties() {
        mergeNull(getFeaturePath("Vehicle", Feature.JsonFields.DESIRED_PROPERTIES.getPointer()));
    }

    @Test
    public void mergeInvalidDesiredFeaturePropertiesAndExpectError() {
        mergeInvalid(getFeaturePath("Vehicle", Feature.JsonFields.DESIRED_PROPERTIES.getPointer()),
                JsonValue.of(Math.PI), "json.invalid");
    }

    @Test
    public void mergeFeatureDesiredProperty() {
        final JsonPointer propertyPath = JsonPointer.of("mergedProperty");
        final JsonValue propertyValue = JsonValue.of(42);
        mergeSuccessful(
                Thing.JsonFields.FEATURES.getPointer()
                        .append(JsonPointer.of("Vehicle"))
                        .append(Feature.JsonFields.DESIRED_PROPERTIES.getPointer())
                        .append(propertyPath),
                propertyValue,
                thing -> thing.setFeatureDesiredProperty("Vehicle", propertyPath, propertyValue));
    }

    @Test
    public void mergeNullFeatureDesiredProperty() {
        mergeNull(Thing.JsonFields.FEATURES.getPointer()
                .append(JsonPointer.of("Vehicle"))
                .append(Feature.JsonFields.DESIRED_PROPERTIES.getPointer())
                .append(JsonPointer.of("status/speed")));
    }

    @Test
    public void mergeThingWithMetadata() {
        final Thing thing = newThing();
        final ThingId thingId = thing.getEntityId().orElseThrow();
        final String metadataKey = "/features/Vehicle/properties/fault/flatTyre/description";
        final String metadataValue = "indicates that the vehicle has a flat tyre";
        final JsonObject metadataObject =
                JsonFactory.newObjectBuilder().set("key", metadataKey).set("value", metadataValue).build();
        final JsonArray jsonMetadata = JsonArray.newBuilder().add(metadataObject).build();
        final JsonObject expectedMetadata = JsonObject.newBuilder()
                .set(Thing.JsonFields.METADATA.getPointer().append(JsonPointer.of(metadataKey)), metadataValue).build();

        patchThing(thingId, JsonPointer.empty(), thing.toJson())
                .withHeader(HttpHeader.PUT_METADATA, jsonMetadata.toString())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        getThing(2, thingId)
                .withFields(JsonFactory.newFieldSelector(Thing.JsonFields.METADATA))
                .expectingBody(contains(expectedMetadata))
                .expectingHttpStatus(HttpStatus.OK)
                .fire();
    }

    @Test
    public void mergeFeaturePropertyWithoutWritePermission() {
        final Thing thing = newThing();
        final ThingId thingId = thing.getEntityId().orElseThrow();

        final Policy policy = PoliciesModelFactory.newPolicy(
                getPolicy(thing.getPolicyId().orElseThrow()).expectingHttpStatus(HttpStatus.OK)
                        .fire()
                        .body()
                        .asString());

        addSubjectWithRevokeToSpecialFeature(policy);

        final JsonPointer propertyPath = JsonPointer.of("mergedProperty");
        final JsonPointer attributePath = JsonPointer.of("newAttribute");
        final JsonValue patchValue = JsonValue.of(42);

        final JsonPointer path = Thing.JsonFields.FEATURES.getPointer()
                .append(JsonPointer.of("Vehicle"))
                .append(Feature.JsonFields.PROPERTIES.getPointer())
                .append(propertyPath);

        patchThing(thingId, Thing.JsonFields.ATTRIBUTES.getPointer().append(attributePath), patchValue)
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        patchThing(thingId, path, patchValue)
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.FORBIDDEN)
                .expectingErrorCode("things:thing.notmodifiable")
                .fire();

        patchThing(thingId, JsonPointer.empty(), JsonFactory.newObject(path, patchValue))
                .withJWT(serviceEnv.getTestingContext2().getOAuthClient().getAccessToken())
                .expectingHttpStatus(HttpStatus.FORBIDDEN)
                .expectingErrorCode("things:thing.notmodifiable")
                .fire();
    }

    private void mergeInvalid(final JsonPointer path, final JsonValue value, final String errorCode) {
        // apply patch at given path
        doMergeExpectingError(path, value, errorCode);
        // additionally apply patch at root level with patch containing the path (should have same result)
        doMergeExpectingError(JsonPointer.empty(), JsonFactory.newObject(path, value), errorCode);
    }

    private void doMergeExpectingError(final JsonPointer path, final JsonValue value, final String errorCode) {
        final Thing thing = newThing();
        final ThingId thingId = thing.getEntityId().orElseThrow();
        LOGGER.info("Applying PATCH to Thing '{}' at '{}': {}", thingId, path, value);
        patchThing(thingId, path, value)
                .expectingHttpStatus(HttpStatus.BAD_REQUEST)
                .expectingErrorCode(errorCode)
                .fire();
    }

    private void mergeSuccessful(final JsonPointer path, final JsonValue value,
            final UnaryOperator<Thing> expectedThingBuilder) {
        // apply patch at given path
        doMergeExpectingSuccess(path, value, expectedThingBuilder);
        // additionally apply patch at root level with patch containing the path (should have same result)
        doMergeExpectingSuccess(JsonPointer.empty(), JsonFactory.newObject(path, value), expectedThingBuilder);
    }

    private void mergeNull(final JsonPointer path) {
        final UnaryOperator<Thing> expectedThingBuilder =
                thing -> {
                    // make sure the existing dummy thing did really contain the value that is deleted
                    assertThat(thing.toJson().getValue(path))
                            .withFailMessage("%s not available in existing Thing: %s", path, thing.toJson())
                            .isNotEmpty();
                    return ThingsModelFactory.newThing(thing.toJson().remove(path));
                };

        // apply patch at given path
        doMergeExpectingSuccess(path, JsonValue.nullLiteral(), expectedThingBuilder);

        // additionally apply patch at root level with patch containing the path (should have same result)
        doMergeExpectingSuccess(JsonPointer.empty(), JsonFactory.newObject(path, JsonValue.nullLiteral()),
                expectedThingBuilder);
    }

    private void doMergeExpectingSuccess(final JsonPointer path, final JsonValue value,
            final UnaryOperator<Thing> expectedThingBuilder) {
        final Thing thing = newThing();
        final ThingId thingId = thing.getEntityId().orElseThrow();
        LOGGER.info("Applying PATCH to Thing '{}' at '{}': {}", thingId, path, value);
        patchThing(thingId, path, value)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
        final Thing patchedThing = getPatchedThing(thingId);
        assertThat(patchedThing).isEqualTo(expectedThingBuilder.apply(thing));
    }

    private static Thing newThing() {
        final Thing dummy = new ThingJsonProducer().getThing();
        final FeatureProperties featureProperties =
                dummy.getFeatures().flatMap(f -> f.getFeature("Vehicle")).flatMap(Feature::getProperties).orElseThrow();
        final Thing thing = dummy.toBuilder()
                .setDefinition(ThingsModelFactory.newDefinition("namespace:name:1.1.1"))
                .setFeatureDefinition("Vehicle",
                        ThingsModelFactory.newFeatureDefinitionBuilder("namespace:name:2.3.4").build())
                .setFeatureDesiredProperties("Vehicle", featureProperties)
                .build();
        return ThingsModelFactory.newThing(postThing(TestConstants.API_V_2, thing.toJson())
                .expectingHttpStatus(HttpStatus.CREATED)
                .fire()
                .body()
                .asString());
    }

    private Thing getPatchedThing(final ThingId thingId) {
        return ThingsModelFactory.newThing(
                getThing(TestConstants.API_V_2, thingId).expectingHttpStatus(HttpStatus.OK)
                        .fire()
                        .body()
                        .asString());
    }

    private JsonPointer getFeaturePath(final String featureId, final JsonPointer featureResource) {
        return Thing.JsonFields.FEATURES.getPointer()
                .append(JsonPointer.of(featureId))
                .append(featureResource);
    }

    private void addSubjectWithRevokeToSpecialFeature(final Policy policy) {
        final Policy updatedPolicy = policy.toBuilder()
                .forLabel("RESTRICTED_LABEL")
                .setSubject(serviceEnv.getTestingContext2().getOAuthClient().getSubject())
                .setResources(Resources.newInstance(
                        Resource.newInstance(PoliciesResourceType.thingResource("/"),
                                EffectedPermissions.newInstance(Arrays.asList("WRITE", "READ"),
                                        Collections.emptyList())),
                        Resource.newInstance(
                                PoliciesResourceType.thingResource("/features/Vehicle/properties/mergedProperty"),
                                EffectedPermissions.newInstance(Collections.emptyList(),
                                        Collections.singletonList("WRITE")))
                )).build();

        putPolicy(updatedPolicy)
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();
    }
}
