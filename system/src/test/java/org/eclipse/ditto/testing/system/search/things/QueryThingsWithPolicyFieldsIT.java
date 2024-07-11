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

import static org.eclipse.ditto.policies.api.Permission.READ;
import static org.eclipse.ditto.policies.api.Permission.WRITE;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isSingleResult;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isSingleResultEqualTo;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.awaitility.Awaitility;
import org.eclipse.ditto.base.model.common.HttpStatus;
import org.eclipse.ditto.base.model.json.FieldType;
import org.eclipse.ditto.base.model.json.JsonSchemaVersion;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonFieldSelector;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.policies.api.Permission;
import org.eclipse.ditto.policies.model.EffectedPermissions;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PoliciesResourceType;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.Resources;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.policies.model.SubjectIssuer;
import org.eclipse.ditto.policies.model.SubjectType;
import org.eclipse.ditto.policies.model.Subjects;
import org.eclipse.ditto.testing.common.SearchIntegrationTest;
import org.eclipse.ditto.testing.common.TestingContext;
import org.eclipse.ditto.testing.common.categories.Acceptance;
import org.eclipse.ditto.things.model.Feature;
import org.eclipse.ditto.things.model.Thing;
import org.eclipse.ditto.things.model.ThingBuilder;
import org.eclipse.ditto.things.model.ThingId;
import org.eclipse.ditto.things.model.ThingsModelFactory;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This Test tests the retrieval of the fields "policyId" and "_policy" via the Search API.
 */
public final class QueryThingsWithPolicyFieldsIT extends SearchIntegrationTest {

    private static final EffectedPermissions RW_PERMISSIONS =
            EffectedPermissions.newInstance(Arrays.asList(READ, WRITE),
                    Collections.emptySet());
    private static final JsonFieldSelector POLICY_FIELDS_SELECTOR =
            JsonFactory.newFieldSelector(Thing.JsonFields.POLICY_ID.getPointer(),
                    JsonPointer.of(Policy.INLINED_FIELD_NAME));

    @Override
    protected void createTestData() {
        // test-data are created per test
    }

    @Test
    public void createThingWithDefaultPolicyInV2AndQueryPolicyFieldsWithV2() {
        // GIVEN
        final Thing thing = createThing();
        final ThingId thingId = thing.getEntityId().orElseThrow(IllegalStateException::new);
        final PolicyId policyId = PolicyId.of(thingId);
        persistThingAndWaitTillAvailable(thing, JsonSchemaVersion.V_2, serviceEnv.getDefaultTestingContext());

        // WHEN / THEN
        final JsonObject expectedSingleResult = createExpectedThing(createDefaultPolicy(policyId));
        searchThings(JsonSchemaVersion.V_2)
                .filter(idFilter(thingId))
                .withFields(POLICY_FIELDS_SELECTOR)
                .expectingBody(isSingleResultEqualTo(expectedSingleResult))
                .fire();
    }

    @Test
    @Category(Acceptance.class)
    public void createThingWithCustomPolicyInV2AndQueryPolicyFieldsWithV2() {
        // GIVEN
        final PolicyId customPolicyId = PolicyId.of(idGenerator().withRandomName());
        final Policy customPolicy = createCustomPolicy(customPolicyId);
        final Thing thing = createThingBuilder().setPolicyId(customPolicyId).build();
        putPolicy(customPolicy).expectingHttpStatus(HttpStatus.CREATED).fire();
        final ThingId thingId = thing.getEntityId().orElseThrow(IllegalStateException::new);
        persistThingAndWaitTillAvailable(thing, JsonSchemaVersion.V_2, serviceEnv.getDefaultTestingContext());

        // WHEN / THEN
        final JsonObject expectedSingleResult = createExpectedThing(customPolicy);
        searchThings(JsonSchemaVersion.V_2)
                .filter(idFilter(thingId))
                .withFields(POLICY_FIELDS_SELECTOR)
                .expectingBody(isSingleResultEqualTo(expectedSingleResult))
                .fire();
    }

    @Test
    public void updatePolicyEntryWithRevokeAndQueryFeatureWildcard() {
        // GIVEN
        final String uuid = UUID.randomUUID().toString();
        final JsonObject featureProperties = JsonObject.newBuilder().set("uuid", uuid).build();
        final Feature feature =
                ThingsModelFactory.newFeature("f", null, ThingsModelFactory.newFeatureProperties(featureProperties));
        final Thing thing = createThingBuilder().setFeature(feature).build();
        final ThingId thingId = thing.getEntityId().orElseThrow();
        final PolicyId policyId = PolicyId.of(thingId);
        persistThingAndWaitTillAvailable(thing, JsonSchemaVersion.V_2, serviceEnv.getDefaultTestingContext());

        final PolicyEntry policyEntry = createPolicyEntryWithGrantOnFeaturesAndRevokeOn1Feature(
                "TEST", serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject(), feature.getId());

        putPolicyEntry(policyId, policyEntry).expectingHttpStatus(HttpStatus.CREATED).fire();

        // WHEN
        searchThings(JsonSchemaVersion.V_2)
                .namespaces(thingId.getNamespace())
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .useAwaitility(Awaitility.await().pollInterval(Duration.ofSeconds(2)).atMost(Duration.ofSeconds(30)))
                .expectingBody(isSingleResult(result -> {}))
                .fire();

        // THEN
        searchThings(JsonSchemaVersion.V_2)
                .filter("eq(features/*/properties/uuid,\"" + uuid + "\")")
                .expectingBody(isSingleResult(result -> {}))
                .fire();
    }

    @Test
    public void addingFeaturePropertyShouldNotDeleteUnchangedProperty() {
        // GIVEN
        final Feature feature0 =
                ThingsModelFactory.newFeature("f", null,
                        ThingsModelFactory.newFeatureProperties(JsonObject.newBuilder().set("m", 0).build()));
        final Feature feature1 =
                ThingsModelFactory.newFeature("f", null, ThingsModelFactory.newFeatureProperties(
                        JsonObject.newBuilder().set("m", 0).set("n", 1).build()));
        final Feature feature2 =
                ThingsModelFactory.newFeature("g", null,
                        ThingsModelFactory.newFeatureProperties(JsonObject.newBuilder().set("k", 2).build()));
        final Thing thing = createThingBuilder().setFeature(feature0).setFeature(feature2).build();
        final ThingId thingId = thing.getEntityId().orElseThrow();
        persistThingAndWaitTillAvailable(thing, JsonSchemaVersion.V_2, serviceEnv.getDefaultTestingContext());

        // WHEN
        putFeature(2, thingId, feature1.getId(), feature1.toJson())
                .withHeader("requested-acks", "[\"search-persisted\"]")
                .expectingHttpStatus(HttpStatus.NO_CONTENT)
                .fire();

        // THEN
        searchThings(JsonSchemaVersion.V_2)
                .filter("eq(features/*/properties/m,0)")
                .expectingBody(isSingleResult(result -> {}))
                .fire();
    }

    public static PolicyEntry createPolicyEntryWithGrantOnFeaturesAndRevokeOn1Feature(final String label,
            final Subject subject,
            final String featureId) {

        final var grants = EffectedPermissions.newInstance(List.of(Permission.READ), List.of());
        final var revokes = EffectedPermissions.newInstance(List.of(), List.of(Permission.READ));
        final List<Resource> resources = List.of(
                PoliciesModelFactory.newResource(PoliciesModelFactory.newResourceKey("thing:/thingId"), grants),
                PoliciesModelFactory.newResource(PoliciesModelFactory.newResourceKey("thing:/features"), grants),
                PoliciesModelFactory.newResource(PoliciesModelFactory.newResourceKey("thing:/features/" + featureId),
                        revokes)
        );
        return PoliciesModelFactory.newPolicyEntry(label, List.of(subject), resources);
    }

    private static Policy createDefaultPolicy(final PolicyId policyId) {
        final Subject subject =
                Subject.newInstance(
                        serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject().getId(),
                        SubjectType.GENERATED);
        final Subjects subjects = Subjects.newInstance(subject);
        final String label = "DEFAULT";
        final Resource policyResource = Resource.newInstance(PoliciesResourceType.policyResource("/"),
                RW_PERMISSIONS);
        final Resource thingResource = Resource.newInstance(PoliciesResourceType.thingResource("/"),
                RW_PERMISSIONS);
        final Resource messageResource = Resource.newInstance(PoliciesResourceType.messageResource("/"),
                RW_PERMISSIONS);
        final Resources resources = Resources.newInstance(policyResource, thingResource, messageResource);
        final PolicyEntry entry = PolicyEntry.newInstance(label,
                subjects, resources);
        return PoliciesModelFactory.newPolicy(policyId, entry);
    }

    private static Subject getSubject(final TestingContext context) {
        if (context.getBasicAuth().isEnabled()) {
            return Subject.newInstance(
                    SubjectIssuer.newInstance("nginx"), context.getBasicAuth().getUsername());
        } else {
            return context.getOAuthClient().getDefaultSubject();
        }
    }

    private static Policy createCustomPolicy(final PolicyId policyId) {
        final Subjects subjects = Subjects.newInstance(getSubject(serviceEnv.getDefaultTestingContext()));
        final String label = "myCustomPolicyEntry";
        final Resource policyResource = Resource.newInstance(PoliciesResourceType.policyResource("/"),
                RW_PERMISSIONS);
        final Resource thingResource = Resource.newInstance(PoliciesResourceType.thingResource("/"),
                RW_PERMISSIONS);
        final Resources resources = Resources.newInstance(policyResource, thingResource);
        final PolicyEntry entry = PolicyEntry.newInstance(label, subjects, resources);
        return PoliciesModelFactory.newPolicy(policyId, entry);
    }

    private JsonObject createExpectedThing(final Policy policy) {
        return ThingsModelFactory.newThingBuilder()
                .setPolicyId(policy.getEntityId().get())
                .build()
                .toJson(POLICY_FIELDS_SELECTOR)
                .setAll(policy.toInlinedJson(JsonSchemaVersion.V_2, FieldType.notHidden()));
    }

    private Thing createThing() {
        return createThingBuilder().build();
    }

    private ThingBuilder.FromScratch createThingBuilder() {
        return ThingsModelFactory.newThingBuilder().setId(ThingId.of(idGenerator().withRandomName()));
    }

}
