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

}
