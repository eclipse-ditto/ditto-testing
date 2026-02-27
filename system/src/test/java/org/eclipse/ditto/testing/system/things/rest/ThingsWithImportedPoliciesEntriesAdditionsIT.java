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
package org.eclipse.ditto.testing.system.things.rest;

import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
import static org.eclipse.ditto.base.model.common.HttpStatus.FORBIDDEN;
import static org.eclipse.ditto.base.model.common.HttpStatus.NOT_FOUND;
import static org.eclipse.ditto.base.model.common.HttpStatus.NO_CONTENT;
import static org.eclipse.ditto.base.model.common.HttpStatus.OK;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.policyResource;
import static org.eclipse.ditto.policies.model.PoliciesResourceType.thingResource;
import static org.eclipse.ditto.things.api.Permission.READ;
import static org.eclipse.ditto.things.api.Permission.WRITE;

import java.util.List;
import java.util.Set;

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
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.TestConstants;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for Thing access via policy import {@code entriesAdditions}.
 * Tests the connectivity use case where a template policy defines resource permissions and an importing
 * policy adds user subjects via {@code entriesAdditions}.
 */
public final class ThingsWithImportedPoliciesEntriesAdditionsIT extends IntegrationTest {

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
    public void secondUserGainsThingAccessViaEntriesAdditions() {
        // Template grants thing:/ READ and allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Importing policy adds user2 subject via entriesAdditions
        final Policy importingPolicy = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId, importedPolicyId, subject2);
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing with the importing policy
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // Verify user2 can access the thing
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void secondUserLosesAccessWhenEntriesAdditionsRemoved() {
        // Template grants thing:/ READ and allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Importing policy adds user2 subject via entriesAdditions
        final Policy importingPolicy = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId, importedPolicyId, subject2);
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing with the importing policy
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // Verify user2 can access the thing initially
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Remove entriesAdditions by updating the import without additions
        final PolicyImport importWithoutAdditions = PoliciesModelFactory.newPolicyImport(importedPolicyId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        putPolicyImport(importingPolicyId, importWithoutAdditions)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Verify user2 loses access
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void templateRevokePreservedWhenResourceAdditionsOverlap() {
        // Template grants thing:/ READ but revokes WRITE, and allows both subject and resource additions
        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());

        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of(WRITE)))),
                ImportableType.IMPLICIT,
                Set.of(AllowedImportAddition.SUBJECTS, AllowedImportAddition.RESOURCES));

        final Policy importedPolicy = PoliciesModelFactory.newPolicyBuilder(importedPolicyId)
                .set(adminEntry)
                .set(defaultEntry)
                .build();
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Importing policy adds subject2 and also tries to add grant WRITE on thing:/
        final Resource additionalResource = PoliciesModelFactory.newResource(thingResource("/"),
                PoliciesModelFactory.newEffectedPermissions(List.of(WRITE), List.of()));
        final EntryAddition entryAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("DEFAULT"),
                PoliciesModelFactory.newSubjects(subject2),
                PoliciesModelFactory.newResources(additionalResource));
        final EntriesAdditions additions = PoliciesModelFactory.newEntriesAdditions(List.of(entryAddition));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")), additions);
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(importedPolicyId, effectedImports);

        final Policy importingPolicy = buildImportingPolicy(importingPolicyId).toBuilder()
                .setPolicyImport(policyImport)
                .build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 should be able to READ (from template grant)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // user2 should NOT be able to WRITE (template revoke should be preserved)
        putThing(TestConstants.API_V_2,
                JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("test", "value").build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void resourceAdditionGrantsWriteAccess() {
        // Template grants thing:/ READ only, allows both subject and resource additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS, AllowedImportAddition.RESOURCES));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Importing policy adds subject2 AND grants WRITE on thing:/ via resource addition
        final Resource writeResource = PoliciesModelFactory.newResource(thingResource("/"),
                PoliciesModelFactory.newEffectedPermissions(List.of(WRITE), List.of()));
        final EntryAddition entryAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("DEFAULT"),
                PoliciesModelFactory.newSubjects(subject2),
                PoliciesModelFactory.newResources(writeResource));
        final EntriesAdditions additions = PoliciesModelFactory.newEntriesAdditions(List.of(entryAddition));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")), additions);
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(importedPolicyId, effectedImports);

        final Policy importingPolicy = buildImportingPolicy(importingPolicyId).toBuilder()
                .setPolicyImport(policyImport)
                .build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 can READ (from template grant)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // user2 can WRITE (from resource addition granting WRITE on thing:/)
        putThing(TestConstants.API_V_2,
                JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("test", "value").build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void additionsForMultipleImportedLabels() {
        // Template has DEFAULT (thing:/ READ) and EXTRA (thing:/ WRITE), both allow subject additions
        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());
        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedImportAddition.SUBJECTS));
        final PolicyEntry extraEntry = PoliciesModelFactory.newPolicyEntry("EXTRA",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(WRITE), List.of()))),
                ImportableType.EXPLICIT, Set.of(AllowedImportAddition.SUBJECTS));

        final Policy importedPolicy = PoliciesModelFactory.newPolicyBuilder(importedPolicyId)
                .set(adminEntry)
                .set(defaultEntry)
                .set(extraEntry)
                .build();
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Importing policy adds subject2 to both DEFAULT and EXTRA labels
        final EntryAddition defaultAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("DEFAULT"),
                PoliciesModelFactory.newSubjects(subject2), null);
        final EntryAddition extraAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("EXTRA"),
                PoliciesModelFactory.newSubjects(subject2), null);
        final EntriesAdditions additions = PoliciesModelFactory.newEntriesAdditions(
                List.of(defaultAddition, extraAddition));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT"), Label.of("EXTRA")), additions);
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(importedPolicyId, effectedImports);

        final Policy importingPolicy = buildImportingPolicy(importingPolicyId).toBuilder()
                .setPolicyImport(policyImport)
                .build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 can READ (from DEFAULT import with subject addition)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // user2 can WRITE (from EXTRA import with subject addition)
        putThing(TestConstants.API_V_2,
                JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("test", "value").build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void templatePermissionChangeBecomesEffectiveForImportingPolicy() {
        // Template grants thing:/ READ only, allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Importing policy adds user2 subject via entriesAdditions
        final Policy importingPolicy = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId, importedPolicyId, subject2);
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing with the importing policy
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 can READ (from template grant)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // user2 cannot WRITE (template only grants READ)
        putThing(TestConstants.API_V_2,
                JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("test", "value").build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // Modify the template policy's DEFAULT entry to grant READ + WRITE
        final PolicyEntry updatedDefaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedImportAddition.SUBJECTS));
        putPolicyEntry(importedPolicyId, updatedDefaultEntry)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // user2 can now WRITE (template change is effective via the importing policy)
        putThing(TestConstants.API_V_2,
                JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("test", "value").build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void reducingAllowedImportAdditionsRevokesResourceAdditionEffect() {
        // Template grants thing:/ READ, allows both subject and resource additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS, AllowedImportAddition.RESOURCES));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Importing policy adds subject2 AND grants WRITE on thing:/ via resource addition
        final Resource writeResource = PoliciesModelFactory.newResource(thingResource("/"),
                PoliciesModelFactory.newEffectedPermissions(List.of(WRITE), List.of()));
        final EntryAddition entryAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("DEFAULT"),
                PoliciesModelFactory.newSubjects(subject2),
                PoliciesModelFactory.newResources(writeResource));
        final EntriesAdditions additions = PoliciesModelFactory.newEntriesAdditions(List.of(entryAddition));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")), additions);
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(importedPolicyId, effectedImports);

        final Policy importingPolicy = buildImportingPolicy(importingPolicyId).toBuilder()
                .setPolicyImport(policyImport)
                .build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 can READ (from template grant) and WRITE (from resource addition)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();
        putThing(TestConstants.API_V_2,
                JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("test", "value").build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Reduce allowedImportAdditions on the template: remove "resources", keep only "subjects"
        final PolicyEntry reducedDefaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedImportAddition.SUBJECTS));
        putPolicyEntry(importedPolicyId, reducedDefaultEntry)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // user2 can still READ (subject addition is still allowed)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // user2 can no longer WRITE (resource addition is no longer applied)
        putThing(TestConstants.API_V_2,
                JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("test", "updated").build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void templateRevokeAddedAfterImportOverridesResourceAdditionGrant() {
        // Template grants thing:/ READ, allows both subject and resource additions
        // DEFAULT entry has no subjects — only entriesAdditions subjects (user2) will be affected by the revoke
        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());
        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT,
                Set.of(AllowedImportAddition.SUBJECTS, AllowedImportAddition.RESOURCES));
        final Policy importedPolicy = PoliciesModelFactory.newPolicyBuilder(importedPolicyId)
                .set(adminEntry)
                .set(defaultEntry)
                .build();
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Importing policy adds subject2 + WRITE resource addition on thing:/
        final Resource writeResource = PoliciesModelFactory.newResource(thingResource("/"),
                PoliciesModelFactory.newEffectedPermissions(List.of(WRITE), List.of()));
        final Policy importingPolicy = buildImportingPolicyWithSubjectAndResourceAdditions(
                importingPolicyId, importedPolicyId, subject2, writeResource);
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 can READ (from template grant)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // user2 can WRITE (from resource addition)
        putThing(TestConstants.API_V_2,
                JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("test", "value").build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Modify template: add explicit REVOKE on WRITE (keep READ grant + allowedAdditions)
        final PolicyEntry updatedDefaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of(WRITE)))),
                ImportableType.IMPLICIT,
                Set.of(AllowedImportAddition.SUBJECTS, AllowedImportAddition.RESOURCES));
        putPolicyEntry(importedPolicyId, updatedDefaultEntry)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // user2 can still READ
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // user2 WRITE is now FORBIDDEN (template revoke overrides resource addition grant)
        putThing(TestConstants.API_V_2,
                JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("test", "updated").build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void templateEntryDeletionRevokesEntriesAdditionsAccess() {
        // Template grants thing:/ READ, allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Importing policy adds user2 subject via entriesAdditions
        final Policy importingPolicy = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId, importedPolicyId, subject2);
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 can READ
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Delete DEFAULT entry from template
        deletePolicyEntry(importedPolicyId, "DEFAULT")
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // user2 loses access
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void templatePolicyDeletionRevokesImportedAccess() {
        // Template grants thing:/ READ, allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Importing policy adds user2 subject via entriesAdditions
        final Policy importingPolicy = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId, importedPolicyId, subject2);
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 can READ
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Delete the entire template policy
        deletePolicy(importedPolicyId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // user2 loses access
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void multipleImportersFromSameTemplateAreIndependentlyAffected() {
        // Template grants thing:/ READ, allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Two importing policies (different IDs), each adds subject2 via entriesAdditions
        final PolicyId importingPolicyId2 = PolicyId.of(idGenerator().withPrefixedRandomName("importing2"));

        final Policy importingPolicy1 = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId, importedPolicyId, subject2);
        putPolicy(importingPolicy1).expectingHttpStatus(CREATED).fire();

        final Policy importingPolicy2 = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId2, importedPolicyId, subject2);
        putPolicy(importingPolicy2).expectingHttpStatus(CREATED).fire();

        // Create two things, one per importing policy
        final String thingId1 = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId1)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        final String thingId2 = importingPolicyId2.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId2)
                        .set("policyId", importingPolicyId2.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 can READ both things
        getThing(TestConstants.API_V_2, thingId1)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();
        getThing(TestConstants.API_V_2, thingId2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Modify template: change DEFAULT to importable=NEVER
        final PolicyEntry neverDefaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.NEVER, Set.of(AllowedImportAddition.SUBJECTS));
        putPolicyEntry(importedPolicyId, neverDefaultEntry)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // user2 loses access to both things
        getThing(TestConstants.API_V_2, thingId1)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();
        getThing(TestConstants.API_V_2, thingId2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId1)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
        deleteThing(TestConstants.API_V_2, thingId2)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void subjectRetainsAccessFromOwnEntryWhenEntriesAdditionsRemoved() {
        // Template grants thing:/ READ, allows subject additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Importing policy: adds subject2 via entriesAdditions AND has direct DIRECT_USER2 entry
        final Policy importingPolicy = buildImportingPolicyWithSubjectAdditions(
                importingPolicyId, importedPolicyId, subject2).toBuilder()
                .forLabel("DIRECT_USER2")
                .setSubject(subject2)
                .setGrantedPermissions(thingResource("/"), READ)
                .build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 can READ
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Remove entriesAdditions (update import without additions)
        final PolicyImport importWithoutAdditions = PoliciesModelFactory.newPolicyImport(importedPolicyId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        putPolicyImport(importingPolicyId, importWithoutAdditions)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // user2 can still READ (from own DIRECT_USER2 entry in importing policy)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void resourceAdditionRespectsSubPathGranularity() {
        // Template grants thing:/ READ, allows both subject and resource additions
        final Policy importedPolicy = buildImportedPolicy(importedPolicyId,
                Set.of(AllowedImportAddition.SUBJECTS, AllowedImportAddition.RESOURCES));
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Importing policy adds subject2 + WRITE resource addition on thing:/attributes only
        final Resource writeAttributesResource = PoliciesModelFactory.newResource(thingResource("/attributes"),
                PoliciesModelFactory.newEffectedPermissions(List.of(WRITE), List.of()));
        final Policy importingPolicy = buildImportingPolicyWithSubjectAndResourceAdditions(
                importingPolicyId, importedPolicyId, subject2, writeAttributesResource);
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing with attributes and features
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("key", "value").build())
                        .set("features", JsonObject.newBuilder()
                                .set("sensor", JsonObject.newBuilder()
                                        .set("properties", JsonObject.newBuilder()
                                                .set("value", 42)
                                                .build())
                                        .build())
                                .build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 can READ whole thing (from template grant on thing:/)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // user2 can WRITE attributes (resource addition grants WRITE on thing:/attributes)
        putAttributes(TestConstants.API_V_2, thingId,
                JsonObject.newBuilder().set("key", "updated").build().toString())
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // user2 cannot WRITE features (no WRITE on thing:/features)
        putFeatures(TestConstants.API_V_2, thingId,
                JsonObject.newBuilder()
                        .set("sensor", JsonObject.newBuilder()
                                .set("properties", JsonObject.newBuilder()
                                        .set("value", 99)
                                        .build())
                                .build())
                        .build().toString())
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void resourceAdditionWithoutSubjectAdditionAppliesToTemplateSubjects() {
        // Template: DEFAULT entry has subject2 as subject with thing:/ READ, allows RESOURCES
        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());
        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(subject2),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedImportAddition.RESOURCES));
        final Policy importedPolicy = PoliciesModelFactory.newPolicyBuilder(importedPolicyId)
                .set(adminEntry)
                .set(defaultEntry)
                .build();
        putPolicy(importedPolicy).expectingHttpStatus(CREATED).fire();

        // Import: WRITE resource addition on thing:/ with no subject addition
        final Resource writeResource = PoliciesModelFactory.newResource(thingResource("/"),
                PoliciesModelFactory.newEffectedPermissions(List.of(WRITE), List.of()));
        final Policy importingPolicy = buildImportingPolicyWithResourceAdditions(
                importingPolicyId, importedPolicyId, writeResource);
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing
        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // user2 can READ (already a template subject)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // user2 can WRITE (resource addition applies to existing template subjects)
        putThing(TestConstants.API_V_2,
                JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("test", "value").build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Cleanup
        deleteThing(TestConstants.API_V_2, thingId)
                .expectingHttpStatus(NO_CONTENT)
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

    private Policy buildImportingPolicyWithResourceAdditions(final PolicyId policyId,
            final PolicyId importedPolicyId, final Resource additionalResource) {

        final EntryAddition entryAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("DEFAULT"),
                null,
                PoliciesModelFactory.newResources(additionalResource));
        final EntriesAdditions additions = PoliciesModelFactory.newEntriesAdditions(List.of(entryAddition));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")), additions);
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(importedPolicyId, effectedImports);

        return buildImportingPolicy(policyId).toBuilder()
                .setPolicyImport(policyImport)
                .build();
    }

}
