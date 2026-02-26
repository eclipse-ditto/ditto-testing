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
package org.eclipse.ditto.testing.system.search.security;

import static org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.policyResource;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.thingResource;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.attribute;
import static org.eclipse.ditto.testing.common.matcher.search.SearchProperties.featureProperty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isCount;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEmpty;
import static org.eclipse.ditto.testing.common.matcher.search.SearchResponseMatchers.isEqualTo;
import static org.eclipse.ditto.things.api.Permission.READ;
import static org.eclipse.ditto.things.api.Permission.WRITE;
import static org.eclipse.ditto.thingsearch.model.SearchModelFactory.and;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.eclipse.ditto.base.model.acks.DittoAcknowledgementLabel;
import org.eclipse.ditto.base.model.headers.DittoHeaderDefinition;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.model.AllowedImportAddition;
import org.eclipse.ditto.policies.model.EffectedImports;
import org.eclipse.ditto.policies.model.EntriesAdditions;
import org.eclipse.ditto.policies.model.EntryAddition;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.PolicyImport;
import org.eclipse.ditto.policies.model.Resource;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.testing.common.SearchIntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.things.model.ThingId;
import org.junit.Before;
import org.junit.Test;

/**
 * Search integration tests verifying that the search index correctly applies authorization based on
 * policy import {@code entriesAdditions}. Uses {@code SEARCH_PERSISTED} acknowledgment for initial
 * consistency and awaitility-based polling for assertions after policy mutations.
 */
public final class SearchWithPolicyImportEntriesAdditionsIT extends SearchIntegrationTest {

    private static final ConditionFactory AWAITILITY_SEARCH_CONFIG =
            Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(3, TimeUnit.SECONDS);

    private PolicyId importedPolicyId;
    private PolicyId importingPolicyId;
    private Subject defaultSubject;
    private Subject subject2;

    @Before
    public void setUp() {
        importedPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("imported"));
        importingPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("importing"));
        defaultSubject = serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject();
        subject2 = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
    }

    @Test
    public void secondUserFindsThingViaSubjectAddition() {
        // Template grants thing:/ READ and allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Importing policy adds user2 subject via entriesAdditions
        final Policy importingPolicy = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId, importedPolicyId, subject2);
        putPolicy(importingPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Create thing with SEARCH_PERSISTED ack
        final ThingId thingId = ThingId.of(importingPolicyId);
        putThingAndWaitForSearchIndex(thingId, importingPolicyId);

        // user2 finds the thing via search
        searchThings(V_2)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        // Also verify count
        searchCount(V_2)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isCount(1))
                .fire();
    }

    @Test
    public void secondUserDoesNotFindThingAfterSubjectAdditionRemoved() {
        // Template grants thing:/ READ and allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Importing policy adds user2 subject via entriesAdditions
        final Policy importingPolicy = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId, importedPolicyId, subject2);
        putPolicy(importingPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Create thing with SEARCH_PERSISTED ack
        final ThingId thingId = ThingId.of(importingPolicyId);
        putThingAndWaitForSearchIndex(thingId, importingPolicyId);

        // Verify user2 finds the thing initially
        searchThings(V_2)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        // Remove entriesAdditions (update import without additions)
        final PolicyImport importWithoutAdditions = PoliciesModelFactory.newPolicyImport(importedPolicyId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        putPolicyImport(importingPolicyId, importWithoutAdditions)
                .expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT)
                .fire();

        // user2 no longer finds the thing
        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void secondUserDoesNotFindThingAfterTemplateEntryDeleted() {
        // Template grants thing:/ READ and allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Importing policy adds user2 subject via entriesAdditions
        final Policy importingPolicy = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId, importedPolicyId, subject2);
        putPolicy(importingPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Create thing with SEARCH_PERSISTED ack
        final ThingId thingId = ThingId.of(importingPolicyId);
        putThingAndWaitForSearchIndex(thingId, importingPolicyId);

        // Verify user2 finds the thing initially
        searchThings(V_2)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        // Delete DEFAULT entry from template
        deletePolicyEntry(importedPolicyId, "DEFAULT")
                .expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT)
                .fire();

        // user2 no longer finds the thing
        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void secondUserDoesNotFindThingAfterTemplateSetToNotImportable() {
        // Template grants thing:/ READ and allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Importing policy adds user2 subject via entriesAdditions
        final Policy importingPolicy = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId, importedPolicyId, subject2);
        putPolicy(importingPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Create thing with SEARCH_PERSISTED ack
        final ThingId thingId = ThingId.of(importingPolicyId);
        putThingAndWaitForSearchIndex(thingId, importingPolicyId);

        // Verify user2 finds the thing initially
        searchThings(V_2)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        // Update template DEFAULT to importable=NEVER
        final PolicyEntry neverDefaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.NEVER, Set.of(AllowedImportAddition.SUBJECTS));
        putPolicyEntry(importedPolicyId, neverDefaultEntry)
                .expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT)
                .fire();

        // user2 no longer finds the thing
        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(idFilter(thingId))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void subjectAdditionWithFineGrainedTemplateGrantAllowsAttributeSearchOnly() {
        // Template grants thing:/attributes READ only (not thing:/), allows subject additions
        final Policy importedPolicy = buildImportedPolicyWithResources(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS),
                List.of(PoliciesModelFactory.newResource(thingResource("/attributes"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))));
        putPolicy(importedPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Importing policy adds user2 via subject addition
        final Policy importingPolicy = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId, importedPolicyId, subject2);
        putPolicy(importingPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Create thing with attributes and features
        final ThingId thingId = ThingId.of(importingPolicyId);
        putThingAndWaitForSearchIndex(buildThingJson(thingId, importingPolicyId));

        // user2 finds the thing by attribute (template grants thing:/attributes READ)
        searchThings(V_2)
                .filter(and(idFilter(thingId), attribute("manufacturer").eq("ACME")))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        // user2 does NOT find the thing by feature property (no READ on thing:/features)
        searchThings(V_2)
                .filter(and(idFilter(thingId), featureProperty("sensor", "temperature").eq(42)))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void resourceAdditionGrantsFeaturePropertySearchAccess() {
        // Template grants thing:/attributes READ, allows subject + resource additions
        final Policy importedPolicy = buildImportedPolicyWithResources(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS, AllowedImportAddition.RESOURCES),
                List.of(PoliciesModelFactory.newResource(thingResource("/attributes"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))));
        putPolicy(importedPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Importing policy adds user2 (subject) + thing:/features READ (resource addition)
        final Resource featuresResource = PoliciesModelFactory.newResource(thingResource("/features"),
                PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()));
        final Policy importingPolicy = buildImportingPolicyWithSubjectAndResourceAdditions(
                importingPolicyId, importedPolicyId, subject2, featuresResource);
        putPolicy(importingPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Create thing with attributes and features
        final ThingId thingId = ThingId.of(importingPolicyId);
        putThingAndWaitForSearchIndex(buildThingJson(thingId, importingPolicyId));

        // user2 finds the thing by attribute (template grant)
        searchThings(V_2)
                .filter(and(idFilter(thingId), attribute("manufacturer").eq("ACME")))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        // user2 finds the thing by feature property (resource addition)
        searchThings(V_2)
                .filter(and(idFilter(thingId), featureProperty("sensor", "temperature").eq(42)))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        // Remove entriesAdditions (update import without additions)
        final PolicyImport importWithoutAdditions = PoliciesModelFactory.newPolicyImport(importedPolicyId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        putPolicyImport(importingPolicyId, importWithoutAdditions)
                .expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT)
                .fire();

        // user2 no longer finds the thing by feature property (resource addition removed)
        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(and(idFilter(thingId), featureProperty("sensor", "temperature").eq(42)))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEmpty())
                .fire();

        // user2 no longer finds the thing by attribute either (subject addition also removed)
        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(and(idFilter(thingId), attribute("manufacturer").eq("ACME")))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void resourceAdditionOnSpecificFeatureAllowsSearchOnlyForThatFeature() {
        // Template grants thing:/attributes READ, allows subject + resource additions
        final Policy importedPolicy = buildImportedPolicyWithResources(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS, AllowedImportAddition.RESOURCES),
                List.of(PoliciesModelFactory.newResource(thingResource("/attributes"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))));
        putPolicy(importedPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Importing policy: subject addition (user2) + resource addition only for sensor properties
        final Resource sensorPropsResource = PoliciesModelFactory.newResource(
                thingResource("/features/sensor/properties"),
                PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()));
        final Policy importingPolicy = buildImportingPolicyWithSubjectAndResourceAdditions(
                importingPolicyId, importedPolicyId, subject2, sensorPropsResource);
        putPolicy(importingPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Create thing with attributes, sensor and actuator features
        final ThingId thingId = ThingId.of(importingPolicyId);
        putThingAndWaitForSearchIndex(buildThingJson(thingId, importingPolicyId));

        // user2 finds the thing by attribute (template grant)
        searchThings(V_2)
                .filter(and(idFilter(thingId), attribute("manufacturer").eq("ACME")))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        // user2 finds the thing by sensor feature property (resource addition on sensor)
        searchThings(V_2)
                .filter(and(idFilter(thingId), featureProperty("sensor", "temperature").eq(42)))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId)))
                .fire();

        // user2 does NOT find the thing by actuator feature property (no READ on actuator)
        searchThings(V_2)
                .filter(and(idFilter(thingId), featureProperty("actuator", "active").eq(true)))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEmpty())
                .fire();
    }

    @Test
    public void multipleThingsFromMultipleImportersAllFoundThenAllLost() {
        // Template grants thing:/ READ and allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Two importing policies, each adds user2 via entriesAdditions
        final PolicyId importingPolicyId2 = PolicyId.of(idGenerator().withPrefixedRandomName("importing2"));

        final Policy importingPolicy1 = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId, importedPolicyId, subject2);
        putPolicy(importingPolicy1).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        final Policy importingPolicy2 = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId2, importedPolicyId, subject2);
        putPolicy(importingPolicy2).expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.CREATED).fire();

        // Create two things (one per importing policy) with SEARCH_PERSISTED ack
        final ThingId thingId1 = ThingId.of(importingPolicyId);
        putThingAndWaitForSearchIndex(thingId1, importingPolicyId);

        final ThingId thingId2 = ThingId.of(importingPolicyId2);
        putThingAndWaitForSearchIndex(thingId2, importingPolicyId2);

        // user2 finds both things
        searchThings(V_2)
                .filter(idsFilter(List.of(thingId1, thingId2)))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEqualTo(toThingResult(thingId1, thingId2)))
                .fire();

        // Modify template: change DEFAULT to importable=NEVER
        final PolicyEntry neverDefaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.NEVER, Set.of(AllowedImportAddition.SUBJECTS));
        putPolicyEntry(importedPolicyId, neverDefaultEntry)
                .expectingHttpStatus(org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT)
                .fire();

        // user2 no longer finds either thing
        searchThings(V_2)
                .useAwaitility(AWAITILITY_SEARCH_CONFIG)
                .filter(idsFilter(List.of(thingId1, thingId2)))
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingBody(isEmpty())
                .fire();
    }

    private void putThingAndWaitForSearchIndex(final ThingId thingId, final PolicyId policyId) {
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId.toString())
                        .set("policyId", policyId.toString())
                        .build(),
                V_2)
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                        DittoAcknowledgementLabel.SEARCH_PERSISTED.toString())
                .withHeader(DittoHeaderDefinition.TIMEOUT.getKey(), "30s")
                .expectingStatusCodeSuccessful()
                .fire();
    }

    private Policy buildImportedPolicy(final PolicyId policyId,
            final Set<AllowedImportAddition> allowedImportAdditions) {

        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());

        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, allowedImportAdditions);

        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .set(adminEntry)
                .set(defaultEntry)
                .build();
    }

    private Policy buildImportingPolicy(final PolicyId policyId) {
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .build();
    }

    private Policy buildImportingPolicyWithSubjectAdditions(final PolicyId policyId,
            final PolicyId importedPolicyId, final Subject additionalSubject) {

        final EntryAddition entryAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("DEFAULT"),
                PoliciesModelFactory.newSubjects(additionalSubject), null);
        final EntriesAdditions additions = PoliciesModelFactory.newEntriesAdditions(List.of(entryAddition));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")), additions);
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(importedPolicyId, effectedImports);

        return buildImportingPolicy(policyId).toBuilder()
                .setPolicyImport(policyImport)
                .build();
    }

    private Policy buildImportingPolicyWithSubjectAndResourceAdditions(final PolicyId policyId,
            final PolicyId importedPolicyId, final Subject additionalSubject,
            final Resource additionalResource) {

        final EntryAddition entryAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("DEFAULT"),
                PoliciesModelFactory.newSubjects(additionalSubject),
                PoliciesModelFactory.newResources(additionalResource));
        final EntriesAdditions additions = PoliciesModelFactory.newEntriesAdditions(List.of(entryAddition));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")), additions);
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(importedPolicyId, effectedImports);

        return buildImportingPolicy(policyId).toBuilder()
                .setPolicyImport(policyImport)
                .build();
    }

    private void putThingAndWaitForSearchIndex(final JsonObject thingJson) {
        putThing(TestConstants.API_V_2, thingJson, V_2)
                .withHeader(DittoHeaderDefinition.REQUESTED_ACKS.getKey(),
                        DittoAcknowledgementLabel.SEARCH_PERSISTED.toString())
                .withHeader(DittoHeaderDefinition.TIMEOUT.getKey(), "30s")
                .expectingStatusCodeSuccessful()
                .fire();
    }

    private Policy buildImportedPolicyWithResources(final PolicyId policyId,
            final Set<AllowedImportAddition> allowedImportAdditions,
            final List<Resource> defaultResources) {

        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());

        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                defaultResources,
                ImportableType.IMPLICIT, allowedImportAdditions);

        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .set(adminEntry)
                .set(defaultEntry)
                .build();
    }

    private static JsonObject buildThingJson(final ThingId thingId, final PolicyId policyId) {
        return JsonObject.newBuilder()
                .set("thingId", thingId.toString())
                .set("policyId", policyId.toString())
                .set("attributes", JsonObject.newBuilder()
                        .set("manufacturer", "ACME")
                        .build())
                .set("features", JsonObject.newBuilder()
                        .set("sensor", JsonObject.newBuilder()
                                .set("properties", JsonObject.newBuilder()
                                        .set("temperature", 42)
                                        .build())
                                .build())
                        .set("actuator", JsonObject.newBuilder()
                                .set("properties", JsonObject.newBuilder()
                                        .set("active", true)
                                        .build())
                                .build())
                        .build())
                .build();
    }

}
