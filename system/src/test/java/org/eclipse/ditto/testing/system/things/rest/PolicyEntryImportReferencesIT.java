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

import org.eclipse.ditto.json.JsonArray;
import org.eclipse.ditto.json.JsonFactory;
import org.eclipse.ditto.json.JsonObject;
import org.eclipse.ditto.policies.model.AllowedAddition;
import org.eclipse.ditto.policies.model.EffectedImports;
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
import org.eclipse.ditto.testing.common.ResourcePathBuilder;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for import references ({@code {"import":"policyId","entry":"label"}}) on policy entries.
 * Verifies that an entry with an import reference inherits resources and namespaces from the referenced
 * imported entry, while subjects remain local to the referencing entry.
 * <p>
 * Migrated from {@code PolicyImportEntriesAdditionsIT} and {@code ThingsWithImportedPoliciesEntriesAdditionsIT}.
 * </p>
 *
 * @since 3.9.0
 */
public final class PolicyEntryImportReferencesIT extends IntegrationTest {

    private PolicyId templatePolicyId;
    private PolicyId importingPolicyId;
    private Subject defaultSubject;
    private Subject subject2;

    @Before
    public void setUp() {
        templatePolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        importingPolicyId = PolicyId.of(idGenerator().withPrefixedRandomName("importing"));
        defaultSubject = serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject();
        subject2 = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
    }

    // ---- Thing access via import reference ----

    @Test
    public void secondUserGainsThingAccessViaImportReference() {
        // Template: DEFAULT grants thing:/ READ, allows subject additions
        createTemplatePolicy(templatePolicyId, Set.of(AllowedAddition.SUBJECTS));

        // Importing policy: "user-access" entry has subject2 + import reference to DEFAULT
        createImportingPolicyWithImportRef(importingPolicyId, templatePolicyId, subject2);

        // Create a thing
        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 can access the thing via inherited resources from import reference
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void secondUserLosesAccessWhenImportReferenceRemoved() {
        createTemplatePolicy(templatePolicyId, Set.of(AllowedAddition.SUBJECTS));
        createImportingPolicyWithImportRef(importingPolicyId, templatePolicyId, subject2);

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 can access
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Remove the import reference
        deleteReferences(importingPolicyId, "user-access")
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 loses access
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void importReferenceInheritsResourcesNotSubjects() {
        createTemplatePolicy(templatePolicyId, Set.of());

        // Importing policy: "user-access" entry has NO subjects, only import reference
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")));
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(templatePolicyId, effectedImports);

        // Entry with import reference but empty subjects
        final JsonObject policyJson = buildImportingPolicyJson(importingPolicyId, templatePolicyId,
                JsonObject.empty(), // no subjects
                JsonArray.of(importRef(templatePolicyId, "DEFAULT")));
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 cannot access (entry has resources from import ref but no subjects)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    // ---- allowedAdditions enforcement ----

    @Test
    public void importRefWithSubjectAdditionsAllowed() {
        createTemplatePolicy(templatePolicyId, Set.of(AllowedAddition.SUBJECTS));
        createImportingPolicyWithImportRef(importingPolicyId, templatePolicyId, subject2);

        getPolicy(importingPolicyId).expectingHttpStatus(OK).fire();
    }

    @Test
    public void importRefWithResourceAdditionsAllowed() {
        createTemplatePolicy(templatePolicyId, Set.of(AllowedAddition.RESOURCES));

        // Entry with extra resource + import reference, but NO own subjects (only RESOURCES allowed)
        final JsonObject policyJson = buildImportingPolicyJson(importingPolicyId, templatePolicyId,
                JsonObject.empty(),
                JsonArray.of(importRef(templatePolicyId, "DEFAULT")),
                JsonObject.newBuilder()
                        .set("thing:/attributes", JsonObject.newBuilder()
                                .set("grant", JsonArray.of(JsonFactory.newValue("READ")))
                                .set("revoke", JsonArray.empty())
                                .build())
                        .build());
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        getPolicy(importingPolicyId).expectingHttpStatus(OK).fire();
    }

    @Test
    public void importRefWithSubjectAndResourceAdditionsAllowed() {
        createTemplatePolicy(templatePolicyId,
                Set.of(AllowedAddition.SUBJECTS, AllowedAddition.RESOURCES));

        final JsonObject policyJson = buildImportingPolicyJsonWithExtraResource(
                importingPolicyId, templatePolicyId, subject2, "thing:/attributes", List.of("READ"));
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        getPolicy(importingPolicyId).expectingHttpStatus(OK).fire();
    }

    @Test
    public void resourceAdditionGrantsWriteAccess() {
        createTemplatePolicy(templatePolicyId,
                Set.of(AllowedAddition.SUBJECTS, AllowedAddition.RESOURCES));

        // Entry adds WRITE resource + subject2 + import reference
        final JsonObject policyJson = buildImportingPolicyJsonWithExtraResource(
                importingPolicyId, templatePolicyId, subject2, "thing:/", List.of("WRITE"));
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 can READ (from template) and WRITE (from own resource addition)
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

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void templateRevokePreservedWhenResourceAdditionsOverlap() {
        // Template: DEFAULT grants READ, revokes WRITE, allows both additions
        final PolicyEntry adminEntry = buildAdminEntry();
        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of(WRITE)))),
                ImportableType.IMPLICIT,
                Set.of(AllowedAddition.SUBJECTS, AllowedAddition.RESOURCES));
        final Policy templatePolicy = PoliciesModelFactory.newPolicyBuilder(templatePolicyId)
                .set(adminEntry).set(defaultEntry).build();
        putPolicy(templatePolicy).expectingHttpStatus(CREATED).fire();

        // Entry adds subject2 + WRITE grant on thing:/ + import reference
        final JsonObject policyJson = buildImportingPolicyJsonWithExtraResource(
                importingPolicyId, templatePolicyId, subject2, "thing:/", List.of("WRITE"));
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 can READ
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // subject2 cannot WRITE (template revoke overrides)
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

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    // ---- Template changes propagate ----

    @Test
    public void templatePermissionChangePropagatesToImportReference() {
        createTemplatePolicy(templatePolicyId, Set.of(AllowedAddition.SUBJECTS));
        createImportingPolicyWithImportRef(importingPolicyId, templatePolicyId, subject2);

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 can READ but not WRITE
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
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        // Update template: grant READ+WRITE
        final PolicyEntry updatedDefaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedAddition.SUBJECTS));
        putPolicyEntry(templatePolicyId, updatedDefaultEntry)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 can now WRITE
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

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void reducingAllowedAdditionsRevokesResourceAdditionEffect() {
        createTemplatePolicy(templatePolicyId,
                Set.of(AllowedAddition.SUBJECTS, AllowedAddition.RESOURCES));

        final JsonObject policyJson = buildImportingPolicyJsonWithExtraResource(
                importingPolicyId, templatePolicyId, subject2, "thing:/", List.of("WRITE"));
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 can READ + WRITE
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

        // Reduce allowedAdditions: remove RESOURCES
        final PolicyEntry reducedEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedAddition.SUBJECTS));
        putPolicyEntry(templatePolicyId, reducedEntry).expectingHttpStatus(NO_CONTENT).fire();

        // subject2 can still READ
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // subject2 can no longer WRITE
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

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void templateRevokeAddedAfterImportOverridesResourceAdditionGrant() {
        // Template: DEFAULT grants READ, allows both additions
        final PolicyEntry adminEntry = buildAdminEntry();
        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT,
                Set.of(AllowedAddition.SUBJECTS, AllowedAddition.RESOURCES));
        final Policy templatePolicy = PoliciesModelFactory.newPolicyBuilder(templatePolicyId)
                .set(adminEntry).set(defaultEntry).build();
        putPolicy(templatePolicy).expectingHttpStatus(CREATED).fire();

        // Entry with subject2 + WRITE resource + import ref
        final JsonObject policyJson = buildImportingPolicyJsonWithExtraResource(
                importingPolicyId, templatePolicyId, subject2, "thing:/", List.of("WRITE"));
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 can WRITE initially
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

        // Template adds WRITE revoke
        final PolicyEntry updatedDefault = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of(WRITE)))),
                ImportableType.IMPLICIT,
                Set.of(AllowedAddition.SUBJECTS, AllowedAddition.RESOURCES));
        putPolicyEntry(templatePolicyId, updatedDefault).expectingHttpStatus(NO_CONTENT).fire();

        // subject2 WRITE now forbidden
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

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    // ---- Template deletion ----

    @Test
    public void templateEntryDeletionRevokesImportReferenceAccess() {
        createTemplatePolicy(templatePolicyId, Set.of(AllowedAddition.SUBJECTS));
        createImportingPolicyWithImportRef(importingPolicyId, templatePolicyId, subject2);

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Delete template DEFAULT entry
        deletePolicyEntry(templatePolicyId, "DEFAULT").expectingHttpStatus(NO_CONTENT).fire();

        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void templatePolicyDeletionRevokesImportReferenceAccess() {
        createTemplatePolicy(templatePolicyId, Set.of(AllowedAddition.SUBJECTS));
        createImportingPolicyWithImportRef(importingPolicyId, templatePolicyId, subject2);

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Delete entire template policy
        deletePolicy(templatePolicyId).expectingHttpStatus(NO_CONTENT).fire();

        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    // ---- Multiple labels / importers ----

    @Test
    public void importRefsForMultipleImportedLabels() {
        // Template: DEFAULT (thing:/ READ) and EXTRA (thing:/ WRITE), both allow subject additions
        final PolicyEntry adminEntry = buildAdminEntry();
        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedAddition.SUBJECTS));
        final PolicyEntry extraEntry = PoliciesModelFactory.newPolicyEntry("EXTRA",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(WRITE), List.of()))),
                ImportableType.EXPLICIT, Set.of(AllowedAddition.SUBJECTS));
        final Policy templatePolicy = PoliciesModelFactory.newPolicyBuilder(templatePolicyId)
                .set(adminEntry).set(defaultEntry).set(extraEntry).build();
        putPolicy(templatePolicy).expectingHttpStatus(CREATED).fire();

        // Two entries in importing policy, each with import ref to different template entries
        final JsonObject policyJson = JsonObject.newBuilder()
                .set("policyId", importingPolicyId.toString())
                .set("imports", JsonObject.newBuilder()
                        .set(templatePolicyId.toString(), JsonObject.newBuilder()
                                .set("entries", JsonFactory.newArrayBuilder()
                                        .add("DEFAULT").add("EXTRA").build())
                                .build())
                        .build())
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", buildAdminEntryJson())
                        .set("read-access", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(subject2))
                                .set("resources", JsonObject.empty())
                                .set("references", JsonArray.of(
                                        importRef(templatePolicyId, "DEFAULT")))
                                .build())
                        .set("write-access", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(subject2))
                                .set("resources", JsonObject.empty())
                                .set("references", JsonArray.of(
                                        importRef(templatePolicyId, "EXTRA")))
                                .build())
                        .build())
                .build();
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 can READ (from DEFAULT ref) and WRITE (from EXTRA ref)
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

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void multipleImportersFromSameTemplateAreIndependent() {
        createTemplatePolicy(templatePolicyId, Set.of(AllowedAddition.SUBJECTS));

        final PolicyId importingPolicyId2 = PolicyId.of(idGenerator().withPrefixedRandomName("importing2"));

        createImportingPolicyWithImportRef(importingPolicyId, templatePolicyId, subject2);
        createImportingPolicyWithImportRef(importingPolicyId2, templatePolicyId, subject2);

        final String thingId1 = importingPolicyId.toString();
        final String thingId2 = importingPolicyId2.toString();
        putThingWithPolicy(thingId1, importingPolicyId);
        putThingWithPolicy(thingId2, importingPolicyId2);

        // subject2 can READ both
        getThing(TestConstants.API_V_2, thingId1)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();
        getThing(TestConstants.API_V_2, thingId2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Change template: importable=NEVER
        final PolicyEntry neverEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.NEVER, Set.of(AllowedAddition.SUBJECTS));
        putPolicyEntry(templatePolicyId, neverEntry).expectingHttpStatus(NO_CONTENT).fire();

        // Both lose access
        getThing(TestConstants.API_V_2, thingId1)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();
        getThing(TestConstants.API_V_2, thingId2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId1).expectingHttpStatus(NO_CONTENT).fire();
        deleteThing(TestConstants.API_V_2, thingId2).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void subjectRetainsAccessFromOwnEntryWhenImportReferenceRemoved() {
        createTemplatePolicy(templatePolicyId, Set.of(AllowedAddition.SUBJECTS));
        createImportingPolicyWithImportRef(importingPolicyId, templatePolicyId, subject2);

        // Add a direct entry for subject2 as well
        final PolicyEntry directEntry = PoliciesModelFactory.newPolicyEntry("DIRECT_USER2",
                List.of(subject2),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.NEVER, Set.of());
        putPolicyEntry(importingPolicyId, directEntry).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Remove import reference
        deleteReferences(importingPolicyId, "user-access")
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 still has access via DIRECT_USER2
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    // ---- Fine-grained resource paths ----

    @Test
    public void resourceAdditionRespectsSubPathGranularity() {
        createTemplatePolicy(templatePolicyId,
                Set.of(AllowedAddition.SUBJECTS, AllowedAddition.RESOURCES));

        // Entry adds WRITE on thing:/attributes only (not features)
        final JsonObject policyJson = buildImportingPolicyJsonWithExtraResource(
                importingPolicyId, templatePolicyId, subject2, "thing:/attributes", List.of("WRITE"));
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("key", "value").build())
                        .set("features", JsonObject.newBuilder()
                                .set("sensor", JsonObject.newBuilder()
                                        .set("properties", JsonObject.newBuilder()
                                                .set("value", 42).build())
                                        .build())
                                .build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2 can READ whole thing (from template)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // subject2 can WRITE attributes
        putAttributes(TestConstants.API_V_2, thingId,
                JsonObject.newBuilder().set("key", "updated").build().toString())
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 cannot WRITE features
        putFeatures(TestConstants.API_V_2, thingId,
                JsonObject.newBuilder()
                        .set("sensor", JsonObject.newBuilder()
                                .set("properties", JsonObject.newBuilder()
                                        .set("value", 99).build())
                                .build())
                        .build().toString())
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(FORBIDDEN)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void resourceAdditionWithoutSubjectAdditionAppliesToTemplateSubjects() {
        // Template: DEFAULT has subject2 as subject with thing:/ READ
        final PolicyEntry adminEntry = buildAdminEntry();
        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(subject2),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedAddition.RESOURCES));
        final Policy templatePolicy = PoliciesModelFactory.newPolicyBuilder(templatePolicyId)
                .set(adminEntry).set(defaultEntry).build();
        putPolicy(templatePolicy).expectingHttpStatus(CREATED).fire();

        // Entry with WRITE resource, no subjects, import ref inherits template's resources
        final JsonObject policyJson = buildImportingPolicyJson(importingPolicyId, templatePolicyId,
                JsonObject.empty(),
                JsonArray.of(importRef(templatePolicyId, "DEFAULT")),
                JsonObject.newBuilder()
                        .set("thing:/", JsonObject.newBuilder()
                                .set("grant", JsonFactory.newArrayBuilder().add("WRITE").build())
                                .set("revoke", JsonArray.empty())
                                .build())
                        .build());
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 can READ (template subject) and WRITE (resource addition)
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

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void changingImportableToNeverRevokesReferencedAccess() {
        createTemplatePolicy(templatePolicyId, Set.of(AllowedAddition.SUBJECTS));
        createImportingPolicyWithImportRef(importingPolicyId, templatePolicyId, subject2);

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Change template to importable=NEVER
        final PolicyEntry neverEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.NEVER, Set.of());
        putPolicyEntry(templatePolicyId, neverEntry).expectingHttpStatus(NO_CONTENT).fire();

        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    // ---- Helpers ----

    private void createTemplatePolicy(final PolicyId policyId,
            final Set<AllowedAddition> allowedAdditions) {

        final PolicyEntry adminEntry = buildAdminEntry();
        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, allowedAdditions);
        final Policy policy = PoliciesModelFactory.newPolicyBuilder(policyId)
                .set(adminEntry).set(defaultEntry).build();
        putPolicy(policy).expectingHttpStatus(CREATED).fire();
    }

    private void createImportingPolicyWithImportRef(final PolicyId policyId,
            final PolicyId templateId, final Subject localSubject) {

        final JsonObject policyJson = buildImportingPolicyJson(policyId, templateId,
                subjectsJson(localSubject),
                JsonArray.of(importRef(templateId, "DEFAULT")));
        putPolicy(policyId, policyJson).expectingHttpStatus(CREATED).fire();
    }

    private PolicyEntry buildAdminEntry() {
        return PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());
    }

    private void putThingWithPolicy(final String thingId, final PolicyId policyId) {
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", policyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();
    }

    private JsonObject buildImportingPolicyJson(final PolicyId policyId, final PolicyId templateId,
            final JsonObject userAccessSubjects, final JsonArray references) {
        return buildImportingPolicyJson(policyId, templateId, userAccessSubjects, references, JsonObject.empty());
    }

    private JsonObject buildImportingPolicyJson(final PolicyId policyId, final PolicyId templateId,
            final JsonObject userAccessSubjects, final JsonArray references,
            final JsonObject extraResources) {

        return JsonObject.newBuilder()
                .set("policyId", policyId.toString())
                .set("imports", JsonObject.newBuilder()
                        .set(templateId.toString(), JsonObject.newBuilder()
                                .set("entries", JsonFactory.newArrayBuilder()
                                        .add("DEFAULT").build())
                                .build())
                        .build())
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", buildAdminEntryJson())
                        .set("user-access", JsonObject.newBuilder()
                                .set("subjects", userAccessSubjects)
                                .set("resources", extraResources)
                                .set("references", references)
                                .build())
                        .build())
                .build();
    }

    private JsonObject buildImportingPolicyJsonWithExtraResource(final PolicyId policyId,
            final PolicyId templateId, final Subject localSubject,
            final String resourcePath, final List<String> grantedPerms) {

        final JsonObject resourcesJson = JsonObject.newBuilder()
                .set(resourcePath, JsonObject.newBuilder()
                        .set("grant", toJsonArray(grantedPerms))
                        .set("revoke", JsonArray.empty())
                        .build())
                .build();

        return buildImportingPolicyJson(policyId, templateId,
                subjectsJson(localSubject),
                JsonArray.of(importRef(templateId, "DEFAULT")),
                resourcesJson);
    }

    private JsonObject buildAdminEntryJson() {
        return JsonObject.newBuilder()
                .set("subjects", subjectsJson(defaultSubject))
                .set("resources", JsonObject.newBuilder()
                        .set("policy:/", JsonObject.newBuilder()
                                .set("grant", JsonFactory.newArrayBuilder()
                                        .add("READ").add("WRITE").build())
                                .set("revoke", JsonArray.empty())
                                .build())
                        .set("thing:/", JsonObject.newBuilder()
                                .set("grant", JsonFactory.newArrayBuilder()
                                        .add("READ").add("WRITE").build())
                                .set("revoke", JsonArray.empty())
                                .build())
                        .build())
                .set("importable", "never")
                .build();
    }

    private static JsonObject subjectsJson(final Subject subject) {
        return JsonObject.newBuilder()
                .set(subject.getId().toString(), subject.toJson())
                .build();
    }

    private static JsonArray toJsonArray(final List<String> strings) {
        final var builder = JsonFactory.newArrayBuilder();
        strings.forEach(builder::add);
        return builder.build();
    }

    private static JsonObject importRef(final PolicyId policyId, final String entryLabel) {
        return JsonObject.newBuilder()
                .set("import", policyId.toString())
                .set("entry", entryLabel)
                .build();
    }

    private static GetMatcher getReferences(final PolicyId policyId, final String label) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyEntryReferences(label).toString();
        return get(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "References");
    }

    private static PutMatcher putReferences(final PolicyId policyId, final String label,
            final JsonArray references) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyEntryReferences(label).toString();
        return put(dittoUrl(TestConstants.API_V_2, path), references.toString())
                .withLogging(LOGGER, "References");
    }

    private static DeleteMatcher deleteReferences(final PolicyId policyId, final String label) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyEntryReferences(label).toString();
        return delete(dittoUrl(TestConstants.API_V_2, path)).withLogging(LOGGER, "References");
    }

}
