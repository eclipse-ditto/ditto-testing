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
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ResourcePathBuilder;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.matcher.GetMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for the {@code transitiveImports} feature of policy imports.
 * <p>
 * Tests validate that multi-level policy import chains are resolved correctly when
 * the {@code transitiveImports} field is used to declare which of the imported policy's
 * own imports should be resolved before extracting entries.
 * </p>
 * Covers:
 * <ul>
 *   <li>CRUD on the transitiveImports field (full import + sub-resource)</li>
 *   <li>Cycle prevention (self-reference rejected)</li>
 *   <li>2-level transitive resolution (3 policies: leaf → intermediate → template)</li>
 *   <li>3-level transitive resolution (4 policies: leaf → dept → regional → template)</li>
 *   <li>Thing access granted/denied through transitive chains</li>
 * </ul>
 *
 * @since 3.9.0
 */
public final class PolicyImportTransitiveImportsIT extends IntegrationTest {

    private Subject defaultSubject;
    private Subject subject2;

    @Before
    public void setUp() {
        defaultSubject = serviceEnv.getDefaultTestingContext().getOAuthClient().getDefaultSubject();
        subject2 = serviceEnv.getTestingContext2().getOAuthClient().getDefaultSubject();
    }

    // ---- CRUD tests ----

    @Test
    public void putPolicyWithTransitiveImportsAndRetrieve() {
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        // Template: DEFAULT entry importable with allowedImportAdditions
        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();

        // Leaf imports template with transitiveImports (even though single-level — tests serialization)
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")),
                null,
                List.of(templateId));
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(templateId, effectedImports);
        final Policy leafPolicy = buildAdminOnlyPolicy(leafId).toBuilder()
                .setPolicyImport(policyImport)
                .build();
        putPolicy(leafPolicy).expectingHttpStatus(CREATED).fire();

        // Retrieve and verify the policy contains transitiveImports
        getPolicy(leafId)
                .expectingHttpStatus(OK)
                .expectingBody(containsCharSequence("transitiveImports"))
                .fire();
    }

    @Test
    public void getAndPutTransitiveImportsSubResource() {
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId intermediateId = PolicyId.of(idGenerator().withPrefixedRandomName("intermediate"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        // Template
        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();

        // Intermediate: imports template with entriesAdditions
        putPolicy(buildIntermediatePolicy(intermediateId, templateId, subject2))
                .expectingHttpStatus(CREATED).fire();

        // Leaf: imports intermediate without transitiveImports initially
        final PolicyImport simpleImport = PoliciesModelFactory.newPolicyImport(intermediateId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        final Policy leafPolicy = buildAdminOnlyPolicy(leafId).toBuilder()
                .setPolicyImport(simpleImport)
                .build();
        putPolicy(leafPolicy).expectingHttpStatus(CREATED).fire();

        // GET transitiveImports — should be empty array
        getTransitiveImports(leafId, intermediateId)
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(JsonArray.empty()))
                .fire();

        // PUT transitiveImports to add template
        final JsonArray transitiveArray = JsonArray.newBuilder()
                .add(templateId.toString())
                .build();
        putTransitiveImports(leafId, intermediateId, transitiveArray)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // GET transitiveImports — should now contain template
        getTransitiveImports(leafId, intermediateId)
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(transitiveArray))
                .fire();

        // PUT empty array to clear transitiveImports
        putTransitiveImports(leafId, intermediateId, JsonArray.empty())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // GET transitiveImports — should be empty again
        getTransitiveImports(leafId, intermediateId)
                .expectingHttpStatus(OK)
                .expectingBody(containsOnly(JsonArray.empty()))
                .fire();
    }

    @Test
    public void transitiveImportOfSelfIsRejected() {
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();

        // Try to create leaf with transitiveImports containing its own ID
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")),
                null,
                List.of(leafId));
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(templateId, effectedImports);
        final Policy leafPolicy = buildAdminOnlyPolicy(leafId).toBuilder()
                .setPolicyImport(policyImport)
                .build();

        putPolicy(leafPolicy)
                .expectingHttpStatus(BAD_REQUEST)
                .expectingErrorCode("policies:import.invalid")
                .fire();
    }

    @Test
    public void transitiveImportOfSelfViaSubResourceIsRejected() {
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();

        // Create leaf with simple import
        final PolicyImport simpleImport = PoliciesModelFactory.newPolicyImport(templateId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        final Policy leafPolicy = buildAdminOnlyPolicy(leafId).toBuilder()
                .setPolicyImport(simpleImport)
                .build();
        putPolicy(leafPolicy).expectingHttpStatus(CREATED).fire();

        // Try to PUT transitiveImports containing the leaf's own ID
        final JsonArray selfReference = JsonArray.newBuilder()
                .add(leafId.toString())
                .build();
        putTransitiveImports(leafId, templateId, selfReference)
                .expectingHttpStatus(BAD_REQUEST)
                .expectingErrorCode("policies:import.invalid")
                .fire();
    }

    // ---- 2-level transitive resolution (3 policies) ----

    @Test
    public void twoLevelTransitiveImportGrantsThingAccess() {
        // Setup: template(C) → intermediate(B) → leaf(A)
        // C: DEFAULT entry with thing:/ READ, allowedImportAdditions=subjects
        // B: imports C, entriesAdditions adds subject2. B has no inline entries.
        // A: imports B with transitiveImports=["C"], entries=["DEFAULT"]
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId intermediateId = PolicyId.of(idGenerator().withPrefixedRandomName("intermediate"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();
        putPolicy(buildIntermediatePolicy(intermediateId, templateId, subject2))
                .expectingHttpStatus(CREATED).fire();
        putPolicy(buildLeafPolicyWithTransitiveImports(leafId, intermediateId, templateId))
                .expectingHttpStatus(CREATED).fire();

        // Create a thing with the leaf policy
        final String thingId = leafId.toString();
        createThing(thingId, leafId);

        // subject2 should have READ access via the transitive chain:
        // A imports B → B's import of C resolved transitively → C's DEFAULT entry + B's subject addition
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void twoLevelImportWithoutTransitiveImportsDeniesAccess() {
        // Same setup as above, but leaf imports intermediate WITHOUT transitiveImports
        // Since intermediate has no inline entries, leaf should get nothing from the import
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId intermediateId = PolicyId.of(idGenerator().withPrefixedRandomName("intermediate"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();
        putPolicy(buildIntermediatePolicy(intermediateId, templateId, subject2))
                .expectingHttpStatus(CREATED).fire();

        // Leaf imports intermediate WITHOUT transitiveImports
        final PolicyImport simpleImport = PoliciesModelFactory.newPolicyImport(intermediateId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        final Policy leafPolicy = buildAdminOnlyPolicy(leafId).toBuilder()
                .setPolicyImport(simpleImport)
                .build();
        putPolicy(leafPolicy).expectingHttpStatus(CREATED).fire();

        final String thingId = leafId.toString();
        createThing(thingId, leafId);

        // subject2 should NOT have access — intermediate has no inline entries
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void addingTransitiveImportsViaSubResourceGrantsAccess() {
        // Start without transitiveImports, add them via sub-resource, verify access is granted
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId intermediateId = PolicyId.of(idGenerator().withPrefixedRandomName("intermediate"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();
        putPolicy(buildIntermediatePolicy(intermediateId, templateId, subject2))
                .expectingHttpStatus(CREATED).fire();

        // Leaf imports intermediate WITHOUT transitiveImports
        final PolicyImport simpleImport = PoliciesModelFactory.newPolicyImport(intermediateId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        final Policy leafPolicy = buildAdminOnlyPolicy(leafId).toBuilder()
                .setPolicyImport(simpleImport)
                .build();
        putPolicy(leafPolicy).expectingHttpStatus(CREATED).fire();

        final String thingId = leafId.toString();
        createThing(thingId, leafId);

        // subject2 has NO access initially
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // Add transitiveImports via sub-resource
        final JsonArray transitiveArray = JsonArray.newBuilder()
                .add(templateId.toString())
                .build();
        putTransitiveImports(leafId, intermediateId, transitiveArray)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 should now have access
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void removingTransitiveImportsRevokesAccess() {
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId intermediateId = PolicyId.of(idGenerator().withPrefixedRandomName("intermediate"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();
        putPolicy(buildIntermediatePolicy(intermediateId, templateId, subject2))
                .expectingHttpStatus(CREATED).fire();
        putPolicy(buildLeafPolicyWithTransitiveImports(leafId, intermediateId, templateId))
                .expectingHttpStatus(CREATED).fire();

        final String thingId = leafId.toString();
        createThing(thingId, leafId);

        // subject2 has access via transitive chain
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Remove transitiveImports via sub-resource
        putTransitiveImports(leafId, intermediateId, JsonArray.empty())
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
    public void templateChangePropagatesToTransitiveConsumer() {
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId intermediateId = PolicyId.of(idGenerator().withPrefixedRandomName("intermediate"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();
        putPolicy(buildIntermediatePolicy(intermediateId, templateId, subject2))
                .expectingHttpStatus(CREATED).fire();
        putPolicy(buildLeafPolicyWithTransitiveImports(leafId, intermediateId, templateId))
                .expectingHttpStatus(CREATED).fire();

        final String thingId = leafId.toString();
        createThing(thingId, leafId);

        // subject2 can READ (template grants thing:/ READ)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Upgrade template: DEFAULT entry now grants READ + WRITE
        final PolicyEntry upgradedDefault = PoliciesModelFactory.newPolicyEntry("DEFAULT",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedImportAddition.SUBJECTS));
        putPolicyEntry(templateId, upgradedDefault)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 can now also WRITE (template change propagated through transitive chain)
        putThing(TestConstants.API_V_2,
                JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", leafId.toString())
                        .set("attributes", JsonObject.newBuilder().set("test", "value").build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void deletingTemplatePolicyRevokesTransitiveAccess() {
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId intermediateId = PolicyId.of(idGenerator().withPrefixedRandomName("intermediate"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();
        putPolicy(buildIntermediatePolicy(intermediateId, templateId, subject2))
                .expectingHttpStatus(CREATED).fire();
        putPolicy(buildLeafPolicyWithTransitiveImports(leafId, intermediateId, templateId))
                .expectingHttpStatus(CREATED).fire();

        final String thingId = leafId.toString();
        createThing(thingId, leafId);

        // subject2 has access
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Delete the template policy
        deletePolicy(templateId)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 loses access (template no longer exists)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    // ---- 3-level transitive resolution (4 policies) ----

    @Test
    public void threeLevelTransitiveImportGrantsThingAccess() {
        // Setup: globalTemplate(D) → regional(C) → department(B) → leaf(A)
        // D: DEFAULT entry with thing:/ READ, allowedImportAdditions=subjects
        // C: imports D, entriesAdditions adds subject2. C has no inline entries.
        // B: imports C with transitiveImports=["D"]. B has no inline entries.
        // A: imports B with transitiveImports=["C"], entries=["DEFAULT"]
        //
        // Resolution chain:
        //   A resolves B → B's import of C resolved (because A declares transitiveImports=["C"])
        //     → C's import of D resolved (because B's import of C has transitiveImports=["D"])
        //       → D's DEFAULT entry loaded → C's entriesAdditions adds subject2
        //     → B gets DEFAULT entry (thing:/ READ + subject2)
        //   → A gets DEFAULT entry
        final PolicyId globalTemplateId = PolicyId.of(idGenerator().withPrefixedRandomName("globalTemplate"));
        final PolicyId regionalId = PolicyId.of(idGenerator().withPrefixedRandomName("regional"));
        final PolicyId departmentId = PolicyId.of(idGenerator().withPrefixedRandomName("department"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        // D: global template
        putPolicy(buildTemplatePolicy(globalTemplateId)).expectingHttpStatus(CREATED).fire();

        // C: regional — imports D with entriesAdditions adding subject2
        putPolicy(buildIntermediatePolicy(regionalId, globalTemplateId, subject2))
                .expectingHttpStatus(CREATED).fire();

        // B: department — imports C with transitiveImports=["D"], no inline entries
        final EffectedImports deptEffectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")),
                null,
                List.of(globalTemplateId));
        final PolicyImport deptImport = PoliciesModelFactory.newPolicyImport(regionalId, deptEffectedImports);
        final Policy departmentPolicy = buildAdminOnlyPolicy(departmentId).toBuilder()
                .setPolicyImport(deptImport)
                .build();
        putPolicy(departmentPolicy).expectingHttpStatus(CREATED).fire();

        // A: leaf — imports B with transitiveImports=["C"], entries=["DEFAULT"]
        final EffectedImports leafEffectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")),
                null,
                List.of(regionalId));
        final PolicyImport leafImport = PoliciesModelFactory.newPolicyImport(departmentId, leafEffectedImports);
        final Policy leafPolicy = buildAdminOnlyPolicy(leafId).toBuilder()
                .setPolicyImport(leafImport)
                .build();
        putPolicy(leafPolicy).expectingHttpStatus(CREATED).fire();

        // Create a thing with the leaf policy
        final String thingId = leafId.toString();
        createThing(thingId, leafId);

        // subject2 should have READ access through the 3-level transitive chain
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void threeLevelTransitiveImportBrokenChainDeniesAccess() {
        // Same 4-policy setup, but B's import of C is MISSING transitiveImports=["D"]
        // Without it, B can only see C's inline entries (which are empty).
        // So even though A has transitiveImports=["C"], the chain breaks at B→C.
        final PolicyId globalTemplateId = PolicyId.of(idGenerator().withPrefixedRandomName("globalTemplate"));
        final PolicyId regionalId = PolicyId.of(idGenerator().withPrefixedRandomName("regional"));
        final PolicyId departmentId = PolicyId.of(idGenerator().withPrefixedRandomName("department"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        putPolicy(buildTemplatePolicy(globalTemplateId)).expectingHttpStatus(CREATED).fire();
        putPolicy(buildIntermediatePolicy(regionalId, globalTemplateId, subject2))
                .expectingHttpStatus(CREATED).fire();

        // B: department — imports C WITHOUT transitiveImports (chain break)
        final PolicyImport deptImport = PoliciesModelFactory.newPolicyImport(regionalId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        final Policy departmentPolicy = buildAdminOnlyPolicy(departmentId).toBuilder()
                .setPolicyImport(deptImport)
                .build();
        putPolicy(departmentPolicy).expectingHttpStatus(CREATED).fire();

        // A: leaf — imports B with transitiveImports=["C"]
        final EffectedImports leafEffectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")),
                null,
                List.of(regionalId));
        final PolicyImport leafImport = PoliciesModelFactory.newPolicyImport(departmentId, leafEffectedImports);
        final Policy leafPolicy = buildAdminOnlyPolicy(leafId).toBuilder()
                .setPolicyImport(leafImport)
                .build();
        putPolicy(leafPolicy).expectingHttpStatus(CREATED).fire();

        final String thingId = leafId.toString();
        createThing(thingId, leafId);

        // subject2 should NOT have access — chain is broken at B→C (no transitive resolution)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void fixingBrokenChainByAddingTransitiveImportsGrantsAccess() {
        // Start with broken 4-level chain, fix it by adding transitiveImports at the department level
        final PolicyId globalTemplateId = PolicyId.of(idGenerator().withPrefixedRandomName("globalTemplate"));
        final PolicyId regionalId = PolicyId.of(idGenerator().withPrefixedRandomName("regional"));
        final PolicyId departmentId = PolicyId.of(idGenerator().withPrefixedRandomName("department"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        putPolicy(buildTemplatePolicy(globalTemplateId)).expectingHttpStatus(CREATED).fire();
        putPolicy(buildIntermediatePolicy(regionalId, globalTemplateId, subject2))
                .expectingHttpStatus(CREATED).fire();

        // B: department — imports C WITHOUT transitiveImports initially
        final PolicyImport deptImport = PoliciesModelFactory.newPolicyImport(regionalId,
                PoliciesModelFactory.newEffectedImportedLabels(List.of(Label.of("DEFAULT"))));
        final Policy departmentPolicy = buildAdminOnlyPolicy(departmentId).toBuilder()
                .setPolicyImport(deptImport)
                .build();
        putPolicy(departmentPolicy).expectingHttpStatus(CREATED).fire();

        // A: leaf — imports B with transitiveImports=["C"]
        final EffectedImports leafEffectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")),
                null,
                List.of(regionalId));
        final PolicyImport leafImport = PoliciesModelFactory.newPolicyImport(departmentId, leafEffectedImports);
        final Policy leafPolicy = buildAdminOnlyPolicy(leafId).toBuilder()
                .setPolicyImport(leafImport)
                .build();
        putPolicy(leafPolicy).expectingHttpStatus(CREATED).fire();

        final String thingId = leafId.toString();
        createThing(thingId, leafId);

        // No access initially (broken chain)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // Fix the chain: add transitiveImports=["D"] to B's import of C
        final JsonArray transitiveArray = JsonArray.newBuilder()
                .add(globalTemplateId.toString())
                .build();
        putTransitiveImports(departmentId, regionalId, transitiveArray)
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 now has access through the fully resolved chain
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void threeLevelTransitiveWithMultipleEntryLabels() {
        // Template has two importable entries: READER (thing:/ READ) and WRITER (thing:/ WRITE)
        // Both allow subject additions. Regional adds subject2 to both.
        // Leaf should get both entries with subject2 through the 3-level chain.
        final PolicyId globalTemplateId = PolicyId.of(idGenerator().withPrefixedRandomName("globalTemplate"));
        final PolicyId regionalId = PolicyId.of(idGenerator().withPrefixedRandomName("regional"));
        final PolicyId departmentId = PolicyId.of(idGenerator().withPrefixedRandomName("department"));
        final PolicyId leafId = PolicyId.of(idGenerator().withPrefixedRandomName("leaf"));

        // D: template with READER and WRITER entries
        final PolicyEntry adminEntry = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());
        final PolicyEntry readerEntry = PoliciesModelFactory.newPolicyEntry("READER",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedImportAddition.SUBJECTS));
        final PolicyEntry writerEntry = PoliciesModelFactory.newPolicyEntry("WRITER",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(WRITE), List.of()))),
                ImportableType.EXPLICIT, Set.of(AllowedImportAddition.SUBJECTS));
        final Policy templatePolicy = PoliciesModelFactory.newPolicyBuilder(globalTemplateId)
                .set(adminEntry)
                .set(readerEntry)
                .set(writerEntry)
                .build();
        putPolicy(templatePolicy).expectingHttpStatus(CREATED).fire();

        // C: regional — imports D, adds subject2 to both READER and WRITER
        final EntryAddition readerAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("READER"), PoliciesModelFactory.newSubjects(subject2), null);
        final EntryAddition writerAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("WRITER"), PoliciesModelFactory.newSubjects(subject2), null);
        final EntriesAdditions additions = PoliciesModelFactory.newEntriesAdditions(
                List.of(readerAddition, writerAddition));
        final EffectedImports regionalEffected = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("READER"), Label.of("WRITER")), additions);
        final PolicyImport regionalImport = PoliciesModelFactory.newPolicyImport(
                globalTemplateId, regionalEffected);
        final Policy regionalPolicy = buildAdminOnlyPolicy(regionalId).toBuilder()
                .setPolicyImport(regionalImport)
                .build();
        putPolicy(regionalPolicy).expectingHttpStatus(CREATED).fire();

        // B: department — imports C with transitiveImports=["D"]
        final EffectedImports deptEffected = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("READER"), Label.of("WRITER")),
                null,
                List.of(globalTemplateId));
        final PolicyImport deptImport = PoliciesModelFactory.newPolicyImport(regionalId, deptEffected);
        final Policy departmentPolicy = buildAdminOnlyPolicy(departmentId).toBuilder()
                .setPolicyImport(deptImport)
                .build();
        putPolicy(departmentPolicy).expectingHttpStatus(CREATED).fire();

        // A: leaf — imports B with transitiveImports=["C"]
        final EffectedImports leafEffected = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("READER"), Label.of("WRITER")),
                null,
                List.of(regionalId));
        final PolicyImport leafImport = PoliciesModelFactory.newPolicyImport(departmentId, leafEffected);
        final Policy leafPolicy = buildAdminOnlyPolicy(leafId).toBuilder()
                .setPolicyImport(leafImport)
                .build();
        putPolicy(leafPolicy).expectingHttpStatus(CREATED).fire();

        final String thingId = leafId.toString();
        createThing(thingId, leafId);

        // subject2 can READ (from READER entry)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // subject2 can WRITE (from WRITER entry)
        putThing(TestConstants.API_V_2,
                JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", leafId.toString())
                        .set("attributes", JsonObject.newBuilder().set("test", "value").build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void multipleConsumersOfSameTransitiveChainAreIndependent() {
        // Two leaf policies both import the same intermediate with transitiveImports.
        // Changing the template affects both, but the leaves are independent.
        final PolicyId templateId = PolicyId.of(idGenerator().withPrefixedRandomName("template"));
        final PolicyId intermediateId = PolicyId.of(idGenerator().withPrefixedRandomName("intermediate"));
        final PolicyId leaf1Id = PolicyId.of(idGenerator().withPrefixedRandomName("leaf1"));
        final PolicyId leaf2Id = PolicyId.of(idGenerator().withPrefixedRandomName("leaf2"));

        putPolicy(buildTemplatePolicy(templateId)).expectingHttpStatus(CREATED).fire();
        putPolicy(buildIntermediatePolicy(intermediateId, templateId, subject2))
                .expectingHttpStatus(CREATED).fire();
        putPolicy(buildLeafPolicyWithTransitiveImports(leaf1Id, intermediateId, templateId))
                .expectingHttpStatus(CREATED).fire();
        putPolicy(buildLeafPolicyWithTransitiveImports(leaf2Id, intermediateId, templateId))
                .expectingHttpStatus(CREATED).fire();

        final String thingId1 = leaf1Id.toString();
        final String thingId2 = leaf2Id.toString();
        createThing(thingId1, leaf1Id);
        createThing(thingId2, leaf2Id);

        // subject2 can access both things
        getThing(TestConstants.API_V_2, thingId1)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();
        getThing(TestConstants.API_V_2, thingId2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Remove transitiveImports from leaf1 only
        putTransitiveImports(leaf1Id, intermediateId, JsonArray.empty())
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 loses access to thing1
        getThing(TestConstants.API_V_2, thingId1)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        // subject2 still has access to thing2
        getThing(TestConstants.API_V_2, thingId2)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId1).expectingHttpStatus(NO_CONTENT).fire();
        deleteThing(TestConstants.API_V_2, thingId2).expectingHttpStatus(NO_CONTENT).fire();
    }

    // ---- Helper methods ----

    /**
     * Builds a template policy with:
     * <ul>
     *   <li>ADMIN entry (policy:/ RW, importable=NEVER)</li>
     *   <li>DEFAULT entry (thing:/ READ, importable=IMPLICIT, allowedImportAdditions=subjects)</li>
     * </ul>
     */
    private Policy buildTemplatePolicy(final PolicyId policyId) {
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
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .set(adminEntry)
                .set(defaultEntry)
                .build();
    }

    /**
     * Builds an intermediate policy that imports the template and adds the given subject
     * via {@code entriesAdditions}. Has no inline thing entries of its own.
     */
    private Policy buildIntermediatePolicy(final PolicyId policyId, final PolicyId templateId,
            final Subject additionalSubject) {
        final EntryAddition entryAddition = PoliciesModelFactory.newEntryAddition(
                Label.of("DEFAULT"),
                PoliciesModelFactory.newSubjects(additionalSubject), null);
        final EntriesAdditions additions = PoliciesModelFactory.newEntriesAdditions(List.of(entryAddition));
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")), additions);
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(templateId, effectedImports);

        return buildAdminOnlyPolicy(policyId).toBuilder()
                .setPolicyImport(policyImport)
                .build();
    }

    /**
     * Builds a leaf policy that imports the intermediate with {@code transitiveImports}
     * pointing to the template, requesting entry label "DEFAULT".
     */
    private Policy buildLeafPolicyWithTransitiveImports(final PolicyId leafId,
            final PolicyId intermediateId, final PolicyId templateId) {
        final EffectedImports effectedImports = PoliciesModelFactory.newEffectedImportedLabels(
                List.of(Label.of("DEFAULT")),
                null,
                List.of(templateId));
        final PolicyImport policyImport = PoliciesModelFactory.newPolicyImport(intermediateId, effectedImports);
        return buildAdminOnlyPolicy(leafId).toBuilder()
                .setPolicyImport(policyImport)
                .build();
    }

    /**
     * Builds a policy with only an ADMIN entry (full access on policy:/ and thing:/).
     */
    private Policy buildAdminOnlyPolicy(final PolicyId policyId) {
        return PoliciesModelFactory.newPolicyBuilder(policyId)
                .forLabel("ADMIN")
                .setSubject(defaultSubject)
                .setGrantedPermissions(policyResource("/"), READ, WRITE)
                .setGrantedPermissions(thingResource("/"), READ, WRITE)
                .build();
    }

    /**
     * Creates a thing with the given thingId and policyId.
     */
    private void createThing(final String thingId, final PolicyId policyId) {
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", policyId.toString())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();
    }

    // ---- Sub-resource helper methods ----

    private static GetMatcher getTransitiveImports(final CharSequence policyId,
            final CharSequence importedPolicyId) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyImport(importedPolicyId).toString() + "/transitiveImports";
        return get(dittoUrl(TestConstants.API_V_2, path))
                .withLogging(LOGGER, "TransitiveImports");
    }

    private static PutMatcher putTransitiveImports(final CharSequence policyId,
            final CharSequence importedPolicyId, final JsonArray transitiveImports) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyImport(importedPolicyId).toString() + "/transitiveImports";
        return put(dittoUrl(TestConstants.API_V_2, path), transitiveImports.toString())
                .withLogging(LOGGER, "TransitiveImports");
    }

}
