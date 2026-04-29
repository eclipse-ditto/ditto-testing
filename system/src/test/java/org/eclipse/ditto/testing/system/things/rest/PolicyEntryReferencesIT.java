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

import static org.eclipse.ditto.base.model.common.HttpStatus.BAD_REQUEST;
import static org.eclipse.ditto.base.model.common.HttpStatus.CONFLICT;
import static org.eclipse.ditto.base.model.common.HttpStatus.CREATED;
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
import org.eclipse.ditto.json.JsonPointer;
import org.eclipse.ditto.policies.model.AllowedAddition;
import org.eclipse.ditto.policies.model.EffectedImports;
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.Label;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.Policy;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.PolicyImport;
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
 * Integration tests for CRUD operations on the {@code /entries/{label}/references} sub-resource
 * and referential integrity validation of policy entry references.
 *
 * @since 3.9.0
 */
public final class PolicyEntryReferencesIT extends IntegrationTest {

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

    // ---- CRUD tests ----

    @Test
    public void getReferencesOnEntryWithoutReferencesReturnsEmpty() {
        final Policy policy = buildAdminOnlyPolicy(importingPolicyId);
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        getReferences(importingPolicyId, "ADMIN")
                .expectingBody(containsOnly(JsonArray.empty()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putImportReferenceAndRetrieveIt() {
        createTemplateAndImportingPolicy();

        final JsonArray refs = JsonArray.of(importRef(templatePolicyId, "DEFAULT"));
        putReferences(importingPolicyId, "user-access", refs)
                .expectingHttpStatus(CREATED)
                .fire();

        getReferences(importingPolicyId, "user-access")
                .expectingBody(containsOnly(refs))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putLocalReferenceAndRetrieveIt() {
        // Policy with two entries: "shared-subjects" and "consumer"
        final Policy policy = buildPolicyWithSharedSubjectsEntry(importingPolicyId);
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        final JsonArray refs = JsonArray.of(localRef("shared-subjects"));
        putReferences(importingPolicyId, "consumer", refs)
                .expectingHttpStatus(CREATED)
                .fire();

        getReferences(importingPolicyId, "consumer")
                .expectingBody(containsOnly(refs))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putMixedReferencesAndRetrieveThem() {
        createTemplatePolicy();

        // Build importing policy with "shared-subjects" + "consumer" entries and an import
        final Policy policy = buildImportingPolicyWithSharedSubjects(importingPolicyId, templatePolicyId);
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        final JsonArray refs = JsonArray.of(
                importRef(templatePolicyId, "DEFAULT"),
                localRef("shared-subjects")
        );
        putReferences(importingPolicyId, "consumer", refs)
                .expectingHttpStatus(CREATED)
                .fire();

        getReferences(importingPolicyId, "consumer")
                .expectingBody(containsOnly(refs))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void deleteReferencesRemovesAll() {
        createTemplateAndImportingPolicy();

        final JsonArray refs = JsonArray.of(importRef(templatePolicyId, "DEFAULT"));
        putReferences(importingPolicyId, "user-access", refs)
                .expectingHttpStatus(CREATED)
                .fire();

        deleteReferences(importingPolicyId, "user-access")
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getReferences(importingPolicyId, "user-access")
                .expectingBody(containsOnly(JsonArray.empty()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putReferencesReplacesExisting() {
        final Policy policy = buildPolicyWithSharedSubjectsEntry(importingPolicyId);
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        // First PUT with local ref — returns 201 (created)
        final JsonArray refs1 = JsonArray.of(localRef("shared-subjects"));
        putReferences(importingPolicyId, "consumer", refs1)
                .expectingHttpStatus(CREATED)
                .fire();

        // Second PUT with empty array replaces
        final JsonArray refs2 = JsonArray.empty();
        putReferences(importingPolicyId, "consumer", refs2)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getReferences(importingPolicyId, "consumer")
                .expectingBody(containsOnly(JsonArray.empty()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void putEmptyArrayClearsReferences() {
        createTemplateAndImportingPolicy();

        final JsonArray refs = JsonArray.of(importRef(templatePolicyId, "DEFAULT"));
        putReferences(importingPolicyId, "user-access", refs)
                .expectingHttpStatus(CREATED)
                .fire();

        putReferences(importingPolicyId, "user-access", JsonArray.empty())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        getReferences(importingPolicyId, "user-access")
                .expectingBody(containsOnly(JsonArray.empty()))
                .expectingHttpStatus(OK)
                .fire();
    }

    @Test
    public void getReferencesOnNonExistentEntryReturns404() {
        final Policy policy = buildAdminOnlyPolicy(importingPolicyId);
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        getReferences(importingPolicyId, "NONEXISTENT")
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    @Test
    public void createPolicyWithEntryReferencesAndRetrieve() {
        createTemplatePolicy();

        // Build importing policy JSON with references in the entry
        final JsonObject policyJson = buildImportingPolicyJsonWithReferences(
                importingPolicyId, templatePolicyId,
                JsonArray.of(importRef(templatePolicyId, "DEFAULT"))
        );
        putPolicy(importingPolicyId, policyJson)
                .expectingHttpStatus(CREATED)
                .fire();

        getReferences(importingPolicyId, "user-access")
                .expectingBody(containsOnly(JsonArray.of(importRef(templatePolicyId, "DEFAULT"))))
                .expectingHttpStatus(OK)
                .fire();
    }

    // ---- Referential integrity tests ----

    @Test
    public void localReferenceToNonExistentEntryReturns400() {
        final Policy policy = buildAdminOnlyPolicy(importingPolicyId);
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        // Add a "consumer" entry first
        final PolicyEntry consumerEntry = PoliciesModelFactory.newPolicyEntry("consumer",
                List.of(subject2),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.NEVER, Set.of());
        putPolicyEntry(importingPolicyId, consumerEntry)
                .expectingHttpStatus(CREATED)
                .fire();

        // Try to reference a non-existent entry
        final JsonArray refs = JsonArray.of(localRef("NONEXISTENT"));
        putReferences(importingPolicyId, "consumer", refs)
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    @Test
    public void importReferenceToUndeclaredImportReturns400() {
        // A real, accessible template policy that we deliberately do NOT declare as an import.
        createTemplatePolicy();

        // Importing policy with no imports declared.
        final Policy policy = buildAdminOnlyPolicy(importingPolicyId);
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        // Add a "consumer" entry
        final PolicyEntry consumerEntry = PoliciesModelFactory.newPolicyEntry("consumer",
                List.of(subject2),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.NEVER, Set.of());
        putPolicyEntry(importingPolicyId, consumerEntry)
                .expectingHttpStatus(CREATED)
                .fire();

        // Reference an existing policy that is not declared in this policy's imports.
        // The pre-enforcer can load the referenced policy successfully; the strategy then
        // rejects with 400 because the import is not declared.
        final JsonArray refs = JsonArray.of(importRef(templatePolicyId, "DEFAULT"));
        putReferences(importingPolicyId, "consumer", refs)
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    @Test
    public void importReferenceToNonExistentPolicyReturns404() {
        // Importing policy with no imports declared.
        final Policy policy = buildAdminOnlyPolicy(importingPolicyId);
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        final PolicyEntry consumerEntry = PoliciesModelFactory.newPolicyEntry("consumer",
                List.of(subject2),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.NEVER, Set.of());
        putPolicyEntry(importingPolicyId, consumerEntry)
                .expectingHttpStatus(CREATED)
                .fire();

        // Referencing a policy that does not exist at all -- the pre-enforcer's load step
        // fails with 404 before the strategy gets a chance to evaluate the imports list.
        final PolicyId nonExistentPolicyId =
                PolicyId.of(idGenerator().withPrefixedRandomName("nonexistent"));
        final JsonArray refs = JsonArray.of(importRef(nonExistentPolicyId, "DEFAULT"));
        putReferences(importingPolicyId, "consumer", refs)
                .expectingHttpStatus(NOT_FOUND)
                .fire();
    }

    @Test
    public void importReferenceToImportableNeverEntryIsRejected() {
        // Template with DEFAULT entry marked NEVER
        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());
        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.NEVER, Set.of());
        final Policy templatePolicy = PoliciesModelFactory.newPolicyBuilder(templatePolicyId)
                .set(adminEntry)
                .set(defaultEntry)
                .build();
        putPolicy(templatePolicy).expectingHttpStatus(CREATED).fire();

        // Importing policy imports template
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")));
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(templatePolicyId, effectedImports);

        final Policy importingPolicy = PoliciesModelFactory.newPolicyBuilder(importingPolicyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .forLabel("user-access")
                .setSubject(subject2)
                .setPolicyImports(PoliciesModelFactory.newPolicyImports(List.of(policyImport)))
                .build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        // Try to add import reference to the NEVER entry
        final JsonArray refs = JsonArray.of(importRef(templatePolicyId, "DEFAULT"));
        putReferences(importingPolicyId, "user-access", refs)
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    @Test
    public void localReferenceToImportableNeverEntryIsRejected() {
        // Build a policy with a NEVER-marked "private" entry plus a "consumer" that tries to
        // local-ref it. Round-2 makes the NEVER signal apply uniformly to local refs as well,
        // and the strategy rejects the write rather than letting the resolver silently skip
        // the reference.
        final Policy policy = PoliciesModelFactory.newPolicyBuilder(importingPolicyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .forLabel("private")
                .setSubject(subject2)
                .setGrantedPermissions(thingResource("/"), READ)
                .setImportable(ImportableType.NEVER)
                .forLabel("consumer")
                .setSubject(defaultSubject)
                .setGrantedPermissions(thingResource("/"), READ)
                .build();
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        final JsonArray refs = JsonArray.of(localRef("private"));
        putReferences(importingPolicyId, "consumer", refs)
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    @Test
    public void shrinkingImportLabelsRejectsWhileEntryReferencesRemovedLabel() {
        // Template with two importable entries (DEFAULT, EXTRA).
        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());
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
        putPolicy(PoliciesModelFactory.newPolicyBuilder(templatePolicyId)
                .set(adminEntry).set(defaultEntry).set(extraEntry).build())
                .expectingHttpStatus(CREATED).fire();

        // Importing policy declares the import with both labels and an entry references EXTRA.
        final EffectedImports both = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT"), Label.of("EXTRA")));
        final PolicyImport importBoth = PoliciesModelFactory.newPolicyImport(templatePolicyId, both);
        final Policy importingPolicy = PoliciesModelFactory.newPolicyBuilder(importingPolicyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .forLabel("user-access")
                .setSubject(subject2)
                .setPolicyImports(PoliciesModelFactory.newPolicyImports(List.of(importBoth)))
                .build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();

        putReferences(importingPolicyId, "user-access", JsonArray.of(importRef(templatePolicyId, "EXTRA")))
                .expectingHttpStatus(CREATED)
                .fire();

        // Try to shrink the import filter to just DEFAULT -- this would orphan the reference
        // to EXTRA. The strategy must reject with 409.
        final EffectedImports onlyDefault = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")));
        final PolicyImport shrunkImport = PoliciesModelFactory.newPolicyImport(templatePolicyId, onlyDefault);
        final String importPath = ResourcePathBuilder.forPolicy(importingPolicyId)
                .policyImport(templatePolicyId).toString();
        put(dittoUrl(TestConstants.API_V_2, importPath), shrunkImport.toJson().toString())
                .expectingHttpStatus(CONFLICT)
                .fire();

        // Removing the reference first allows the shrink to succeed.
        deleteReferences(importingPolicyId, "user-access")
                .expectingHttpStatus(NO_CONTENT)
                .fire();
        put(dittoUrl(TestConstants.API_V_2, importPath), shrunkImport.toJson().toString())
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void selfReferencingLocalEntryIsRejected() {
        final Policy policy = buildAdminOnlyPolicy(importingPolicyId);
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        // Add a "consumer" entry
        final PolicyEntry consumerEntry = PoliciesModelFactory.newPolicyEntry("consumer",
                List.of(subject2),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.NEVER, Set.of());
        putPolicyEntry(importingPolicyId, consumerEntry)
                .expectingHttpStatus(CREATED)
                .fire();

        // Try to self-reference
        final JsonArray refs = JsonArray.of(localRef("consumer"));
        putReferences(importingPolicyId, "consumer", refs)
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    @Test
    public void deleteImportReferencedByEntryReturns409() {
        createTemplateAndImportingPolicy();

        // Add import reference
        final JsonArray refs = JsonArray.of(importRef(templatePolicyId, "DEFAULT"));
        putReferences(importingPolicyId, "user-access", refs)
                .expectingHttpStatus(CREATED)
                .fire();

        // Try to delete the import - should fail with 409
        final String importPath = ResourcePathBuilder.forPolicy(importingPolicyId)
                .policyImport(templatePolicyId).toString();
        delete(dittoUrl(TestConstants.API_V_2, importPath))
                .expectingHttpStatus(CONFLICT)
                .fire();
    }

    @Test
    public void deleteImportSucceedsAfterRemovingReferences() {
        createTemplateAndImportingPolicy();

        // Add import reference
        final JsonArray refs = JsonArray.of(importRef(templatePolicyId, "DEFAULT"));
        putReferences(importingPolicyId, "user-access", refs)
                .expectingHttpStatus(CREATED)
                .fire();

        // Remove references first
        deleteReferences(importingPolicyId, "user-access")
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Now deleting the import should succeed
        final String importPath = ResourcePathBuilder.forPolicy(importingPolicyId)
                .policyImport(templatePolicyId).toString();
        delete(dittoUrl(TestConstants.API_V_2, importPath))
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void deleteEntryReferencedByLocalReferenceReturns409() {
        final Policy policy = buildPolicyWithSharedSubjectsEntry(importingPolicyId);
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        // Add local reference from consumer to shared-subjects
        final JsonArray refs = JsonArray.of(localRef("shared-subjects"));
        putReferences(importingPolicyId, "consumer", refs)
                .expectingHttpStatus(CREATED)
                .fire();

        // Try to delete shared-subjects - should fail with 409
        deletePolicyEntry(importingPolicyId, "shared-subjects")
                .expectingHttpStatus(CONFLICT)
                .fire();
    }

    @Test
    public void deleteEntrySucceedsAfterRemovingLocalReferences() {
        final Policy policy = buildPolicyWithSharedSubjectsEntry(importingPolicyId);
        putPolicy(policy).expectingHttpStatus(CREATED).fire();

        // Add local reference
        final JsonArray refs = JsonArray.of(localRef("shared-subjects"));
        putReferences(importingPolicyId, "consumer", refs)
                .expectingHttpStatus(CREATED)
                .fire();

        // Remove local reference first
        deleteReferences(importingPolicyId, "consumer")
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // Now deleting the entry should succeed
        deletePolicyEntry(importingPolicyId, "shared-subjects")
                .expectingHttpStatus(NO_CONTENT)
                .fire();
    }

    @Test
    public void createPolicyWithBrokenLocalReferenceReturns400() {
        // Build policy JSON with entry A referencing non-existent entry B
        final JsonObject policyJson = JsonObject.newBuilder()
                .set("policyId", importingPolicyId.toString())
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", JsonObject.newBuilder()
                                .set("subjects", JsonObject.newBuilder()
                                        .set(defaultSubject.getId().toString(), defaultSubject.toJson())
                                        .build())
                                .set("resources", JsonObject.newBuilder()
                                        .set("policy:/", JsonObject.newBuilder()
                                                .set("grant", JsonFactory.newArrayBuilder()
                                                        .add("READ").add("WRITE").build())
                                                .set("revoke", JsonArray.empty())
                                                .build())
                                        .build())
                                .set("importable", "never")
                                .build())
                        .set("consumer", JsonObject.newBuilder()
                                .set("subjects", JsonObject.newBuilder()
                                        .set(subject2.getId().toString(), subject2.toJson())
                                        .build())
                                .set("resources", JsonObject.newBuilder()
                                        .set("thing:/", JsonObject.newBuilder()
                                                .set("grant", JsonFactory.newArrayBuilder().add("READ").build())
                                                .set("revoke", JsonArray.empty())
                                                .build())
                                        .build())
                                .set("references", JsonArray.of(localRef("NONEXISTENT")))
                                .build())
                        .build())
                .build();

        putPolicy(importingPolicyId, policyJson)
                .expectingHttpStatus(BAD_REQUEST)
                .fire();
    }

    // ---- Helpers ----

    private void createTemplatePolicy() {
        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());
        final PolicyEntry defaultEntry = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedAddition.SUBJECTS));
        final Policy templatePolicy = PoliciesModelFactory.newPolicyBuilder(templatePolicyId)
                .set(adminEntry)
                .set(defaultEntry)
                .build();
        putPolicy(templatePolicy).expectingHttpStatus(CREATED).fire();
    }

    private void createTemplateAndImportingPolicy() {
        createTemplatePolicy();

        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")));
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(templatePolicyId, effectedImports);

        // user-access has subject2 only; resources come via import reference
        final Policy importingPolicy = PoliciesModelFactory.newPolicyBuilder(importingPolicyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .forLabel("user-access")
                .setSubject(subject2)
                .setPolicyImports(PoliciesModelFactory.newPolicyImports(List.of(policyImport)))
                .build();
        putPolicy(importingPolicy).expectingHttpStatus(CREATED).fire();
    }

    private Policy buildAdminOnlyPolicy(final PolicyId policyId) {
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .build();
    }

    private Policy buildPolicyWithSharedSubjectsEntry(final PolicyId policyId) {
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .forLabel("shared-subjects")
                .setSubject(subject2)
                .setGrantedPermissions(thingResource("/"), READ)
                .forLabel("consumer")
                .setSubject(defaultSubject)
                .setGrantedPermissions(thingResource("/"), READ)
                .build();
    }

    private Policy buildImportingPolicyWithSharedSubjects(final PolicyId policyId,
            final PolicyId templateId) {

        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")));
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(templateId, effectedImports);

        // consumer has no own resources; they come via references
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .forLabel("shared-subjects")
                .setSubject(subject2)
                .setGrantedPermissions(thingResource("/"), READ)
                .forLabel("consumer")
                .setSubject(defaultSubject)
                .setPolicyImports(PoliciesModelFactory.newPolicyImports(List.of(policyImport)))
                .build();
    }

    private JsonObject buildImportingPolicyJsonWithReferences(final PolicyId policyId,
            final PolicyId templateId, final JsonArray references) {

        return JsonObject.newBuilder()
                .set("policyId", policyId.toString())
                .set("imports", JsonObject.newBuilder()
                        .set(templateId.toString(), JsonObject.newBuilder()
                                .set("entries", JsonFactory.newArrayBuilder()
                                        .add("DEFAULT").build())
                                .build())
                        .build())
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", JsonObject.newBuilder()
                                .set("subjects", JsonObject.newBuilder()
                                        .set(defaultSubject.getId().toString(), defaultSubject.toJson())
                                        .build())
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
                                .build())
                        .set("user-access", JsonObject.newBuilder()
                                .set("subjects", JsonObject.newBuilder()
                                        .set(subject2.getId().toString(), subject2.toJson())
                                        .build())
                                .set("resources", JsonObject.empty())
                                .set("references", references)
                                .build())
                        .build())
                .build();
    }

    private static JsonObject importRef(final PolicyId policyId, final String entryLabel) {
        return JsonObject.newBuilder()
                .set("import", policyId.toString())
                .set("entry", entryLabel)
                .build();
    }

    private static JsonObject localRef(final String entryLabel) {
        return JsonObject.newBuilder()
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
