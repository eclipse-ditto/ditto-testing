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
import org.eclipse.ditto.policies.model.ImportableType;
import org.eclipse.ditto.policies.model.PoliciesModelFactory;
import org.eclipse.ditto.policies.model.PolicyEntry;
import org.eclipse.ditto.policies.model.PolicyId;
import org.eclipse.ditto.policies.model.Subject;
import org.eclipse.ditto.testing.common.IntegrationTest;
import org.eclipse.ditto.testing.common.ResourcePathBuilder;
import org.eclipse.ditto.testing.common.TestConstants;
import org.eclipse.ditto.testing.common.matcher.DeleteMatcher;
import org.eclipse.ditto.testing.common.matcher.PutMatcher;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests for policy entries that combine both import references and local references.
 * Verifies additive resolution of resources (from import refs) and subjects (from local refs)
 * on the same entry, as well as reference chaining.
 *
 * @since 3.9.0
 */
public final class PolicyEntryCombinedReferencesIT extends IntegrationTest {

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

    @Test
    public void entryWithImportAndLocalReferenceMergesBoth() {
        // Template: DEFAULT grants thing:/ READ
        createTemplatePolicy(templatePolicyId);

        // Importing policy:
        //   "shared-subjects" has subject2 (no resources)
        //   "consumer" has no own subjects/resources, references both import + local
        final JsonObject policyJson = JsonObject.newBuilder()
                .set("policyId", importingPolicyId.toString())
                .set("imports", importsJson(templatePolicyId, "DEFAULT"))
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", buildAdminEntryJson())
                        .set("shared-subjects", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(subject2))
                                .set("resources", JsonObject.empty())
                                .build())
                        .set("consumer", JsonObject.newBuilder()
                                .set("subjects", JsonObject.empty())
                                .set("resources", JsonObject.empty())
                                .set("references", JsonArray.of(
                                        importRef(templatePolicyId, "DEFAULT"),
                                        localRef("shared-subjects")))
                                .build())
                        .build())
                .build();
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 can access: subjects from local ref, resources from import ref
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void entryWithOwnSubjectsAndImportReference() {
        // This is the canonical migration pattern from entriesAdditions:
        // Entry has subject2 as own subject + import reference for resources
        createTemplatePolicy(templatePolicyId);

        final JsonObject policyJson = JsonObject.newBuilder()
                .set("policyId", importingPolicyId.toString())
                .set("imports", importsJson(templatePolicyId, "DEFAULT"))
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", buildAdminEntryJson())
                        .set("user-access", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(subject2))
                                .set("resources", JsonObject.empty())
                                .set("references", JsonArray.of(
                                        importRef(templatePolicyId, "DEFAULT")))
                                .build())
                        .build())
                .build();
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 can access: own subject + resources from import ref
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void entryWithOwnResourcesAndLocalReference() {
        // "shared-subjects" has subject2
        // "operator" has own thing:/ READ+WRITE + references shared-subjects for subjects
        final JsonObject policyJson = JsonObject.newBuilder()
                .set("policyId", importingPolicyId.toString())
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", buildAdminEntryJson())
                        .set("shared-subjects", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(subject2))
                                .set("resources", JsonObject.empty())
                                .build())
                        .set("operator", JsonObject.newBuilder()
                                .set("subjects", JsonObject.empty())
                                .set("resources", resourcesJson("thing:/", List.of("READ", "WRITE"), List.of()))
                                .set("references", JsonArray.of(localRef("shared-subjects")))
                                .build())
                        .build())
                .build();
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 can read+write
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
    public void removingImportRefWhileKeepingLocalRefPreservesSubjects() {
        createTemplatePolicy(templatePolicyId);

        // Entry with both import ref (resources) and local ref (subjects)
        final JsonObject policyJson = JsonObject.newBuilder()
                .set("policyId", importingPolicyId.toString())
                .set("imports", importsJson(templatePolicyId, "DEFAULT"))
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", buildAdminEntryJson())
                        .set("shared-subjects", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(subject2))
                                .set("resources", JsonObject.empty())
                                .build())
                        .set("consumer", JsonObject.newBuilder()
                                .set("subjects", JsonObject.empty())
                                .set("resources", JsonObject.empty())
                                .set("references", JsonArray.of(
                                        importRef(templatePolicyId, "DEFAULT"),
                                        localRef("shared-subjects")))
                                .build())
                        .build())
                .build();
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 can access
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // Remove import ref, keep only local ref
        putReferences(importingPolicyId, "consumer", JsonArray.of(localRef("shared-subjects")))
                .expectingHttpStatus(NO_CONTENT)
                .fire();

        // subject2 loses access (no resources after resolution - entry filtered out)
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void twoLevelTransitiveWithImportReferencesGrantsAccess() {
        // 3-policy chain: template -> intermediate -> leaf
        // Template (C): DEFAULT grants thing:/ READ, importable=IMPLICIT
        final PolicyId intermediateId = PolicyId.of(idGenerator().withPrefixedRandomName("intermediate"));
        createTemplatePolicy(templatePolicyId);

        // Intermediate (B): imports C, has "user-access" entry with subject2 + import ref to C:DEFAULT
        final JsonObject intermediateJson = JsonObject.newBuilder()
                .set("policyId", intermediateId.toString())
                .set("imports", importsJson(templatePolicyId, "DEFAULT"))
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", buildAdminEntryJson())
                        .set("user-access", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(subject2))
                                .set("resources", JsonObject.empty())
                                .set("references", JsonArray.of(
                                        importRef(templatePolicyId, "DEFAULT")))
                                .set("importable", "implicit")
                                .build())
                        .build())
                .build();
        putPolicy(intermediateId, intermediateJson).expectingHttpStatus(CREATED).fire();

        // Leaf (A): imports B with transitiveImports=[C], has entry with import ref to B:user-access
        final JsonObject leafJson = JsonObject.newBuilder()
                .set("policyId", importingPolicyId.toString())
                .set("imports", JsonObject.newBuilder()
                        .set(intermediateId.toString(), JsonObject.newBuilder()
                                .set("entries", JsonFactory.newArrayBuilder()
                                        .add("user-access").build())
                                .set("transitiveImports", JsonFactory.newArrayBuilder()
                                        .add(templatePolicyId.toString()).build())
                                .build())
                        .build())
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", buildAdminEntryJson())
                        .build())
                .build();
        putPolicy(importingPolicyId, leafJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThingWithPolicy(thingId, importingPolicyId);

        // subject2 can access through the transitive chain
        getThing(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void threeImportRefsToEntriesFromDifferentImportedPolicies() {
        // Template-A: grants thing:/attributes READ
        final PolicyId templateAId = PolicyId.of(idGenerator().withPrefixedRandomName("templateA"));
        final PolicyEntry adminEntryA = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());
        final PolicyEntry attrEntry = PoliciesModelFactory.newPolicyEntry("ATTR_ACCESS",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/attributes"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedAddition.SUBJECTS));
        putPolicy(PoliciesModelFactory.newPolicyBuilder(templateAId)
                .set(adminEntryA).set(attrEntry).build())
                .expectingHttpStatus(CREATED).fire();

        // Template-B: grants thing:/features READ
        final PolicyId templateBId = PolicyId.of(idGenerator().withPrefixedRandomName("templateB"));
        final PolicyEntry adminEntryB = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());
        final PolicyEntry featEntry = PoliciesModelFactory.newPolicyEntry("FEAT_ACCESS",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/features"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedAddition.SUBJECTS));
        putPolicy(PoliciesModelFactory.newPolicyBuilder(templateBId)
                .set(adminEntryB).set(featEntry).build())
                .expectingHttpStatus(CREATED).fire();

        // Importing policy: imports both templates, entry references both
        final JsonObject policyJson = JsonObject.newBuilder()
                .set("policyId", importingPolicyId.toString())
                .set("imports", JsonObject.newBuilder()
                        .set(templateAId.toString(), JsonObject.newBuilder()
                                .set("entries", JsonFactory.newArrayBuilder()
                                        .add("ATTR_ACCESS").build())
                                .build())
                        .set(templateBId.toString(), JsonObject.newBuilder()
                                .set("entries", JsonFactory.newArrayBuilder()
                                        .add("FEAT_ACCESS").build())
                                .build())
                        .build())
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", buildAdminEntryJson())
                        .set("combined-access", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(subject2))
                                .set("resources", JsonObject.empty())
                                .set("references", JsonArray.of(
                                        importRef(templateAId, "ATTR_ACCESS"),
                                        importRef(templateBId, "FEAT_ACCESS")))
                                .build())
                        .build())
                .build();
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("key", "value").build())
                        .set("features", JsonObject.newBuilder()
                                .set("sensor", JsonObject.newBuilder()
                                        .set("properties", JsonObject.newBuilder()
                                                .set("temp", 22).build())
                                        .build())
                                .build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2 can read attributes (from template-A)
        getAttributes(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        // subject2 can read features (from template-B)
        getFeatures(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(OK)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    @Test
    public void multipleImportsWithDisjointAllowedAdditionsStripOwnAdditionsAtRuntime() {
        // Two templates with disjoint allowedAdditions:
        //   Template-A: allows SUBJECTS only (grants thing:/attributes READ)
        //   Template-B: allows RESOURCES only (grants thing:/features READ)
        // The strictest intersection across both imports is empty -> the consumer entry's
        // own subject AND own resource must both be silently stripped at enforcement time.
        final PolicyId templateAId = PolicyId.of(idGenerator().withPrefixedRandomName("templateA"));
        final PolicyEntry adminEntryA = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());
        final PolicyEntry attrEntry = PoliciesModelFactory.newPolicyEntry("ATTR_ACCESS",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/attributes"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedAddition.SUBJECTS));
        putPolicy(PoliciesModelFactory.newPolicyBuilder(templateAId)
                .set(adminEntryA).set(attrEntry).build())
                .expectingHttpStatus(CREATED).fire();

        final PolicyId templateBId = PolicyId.of(idGenerator().withPrefixedRandomName("templateB"));
        final PolicyEntry adminEntryB = PoliciesModelFactory.newPolicyEntry("ADMIN",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(policyResource("/"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ, WRITE), List.of()))),
                ImportableType.NEVER, Set.of());
        final PolicyEntry featEntry = PoliciesModelFactory.newPolicyEntry("FEAT_ACCESS",
                List.of(defaultSubject),
                List.of(PoliciesModelFactory.newResource(thingResource("/features"),
                        PoliciesModelFactory.newEffectedPermissions(List.of(READ), List.of()))),
                ImportableType.IMPLICIT, Set.of(AllowedAddition.RESOURCES));
        putPolicy(PoliciesModelFactory.newPolicyBuilder(templateBId)
                .set(adminEntryB).set(featEntry).build())
                .expectingHttpStatus(CREATED).fire();

        // Importing policy: combined-access has subject2 (own subject) AND a WRITE on thing:/
        // (own resource), plus import refs to both templates.
        final JsonObject policyJson = JsonObject.newBuilder()
                .set("policyId", importingPolicyId.toString())
                .set("imports", JsonObject.newBuilder()
                        .set(templateAId.toString(), JsonObject.newBuilder()
                                .set("entries", JsonFactory.newArrayBuilder()
                                        .add("ATTR_ACCESS").build())
                                .build())
                        .set(templateBId.toString(), JsonObject.newBuilder()
                                .set("entries", JsonFactory.newArrayBuilder()
                                        .add("FEAT_ACCESS").build())
                                .build())
                        .build())
                .set("entries", JsonObject.newBuilder()
                        .set("ADMIN", buildAdminEntryJson())
                        .set("combined-access", JsonObject.newBuilder()
                                .set("subjects", subjectsJson(subject2))
                                .set("resources", resourcesJson("thing:/", List.of("WRITE"), List.of()))
                                .set("references", JsonArray.of(
                                        importRef(templateAId, "ATTR_ACCESS"),
                                        importRef(templateBId, "FEAT_ACCESS")))
                                .build())
                        .build())
                .build();
        // Write succeeds -- allowedAdditions is a runtime filter, not a write-time contract.
        putPolicy(importingPolicyId, policyJson).expectingHttpStatus(CREATED).fire();

        final String thingId = importingPolicyId.toString();
        putThing(TestConstants.API_V_2, JsonObject.newBuilder()
                        .set("thingId", thingId)
                        .set("policyId", importingPolicyId.toString())
                        .set("attributes", JsonObject.newBuilder().set("k", "v").build())
                        .set("features", JsonObject.newBuilder()
                                .set("sensor", JsonObject.newBuilder()
                                        .set("properties", JsonObject.newBuilder()
                                                .set("temp", 22).build())
                                        .build())
                                .build())
                        .build(),
                org.eclipse.ditto.base.model.json.JsonSchemaVersion.V_2)
                .expectingHttpStatus(CREATED)
                .fire();

        // subject2's own subject is stripped at runtime (not in B's allowedAdditions).
        // No template binds subject2, so subject2 has no effective access at all.
        getAttributes(TestConstants.API_V_2, thingId)
                .withConfiguredAuth(serviceEnv.getTestingContext2())
                .expectingHttpStatus(NOT_FOUND)
                .fire();

        deleteThing(TestConstants.API_V_2, thingId).expectingHttpStatus(NO_CONTENT).fire();
    }

    // ---- Helpers ----

    private void createTemplatePolicy(final PolicyId policyId) {
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
        putPolicy(PoliciesModelFactory.newPolicyBuilder(policyId)
                .set(adminEntry).set(defaultEntry).build())
                .expectingHttpStatus(CREATED).fire();
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

    private static JsonObject importsJson(final PolicyId templateId, final String... entries) {
        final var arrayBuilder = JsonFactory.newArrayBuilder();
        for (final String entry : entries) {
            arrayBuilder.add(entry);
        }
        return JsonObject.newBuilder()
                .set(templateId.toString(), JsonObject.newBuilder()
                        .set("entries", arrayBuilder.build())
                        .build())
                .build();
    }

    private static JsonObject subjectsJson(final Subject subject) {
        return JsonObject.newBuilder()
                .set(subject.getId().toString(), subject.toJson())
                .build();
    }

    private static JsonObject resourcesJson(final String path, final List<String> grant,
            final List<String> revoke) {
        return JsonObject.newBuilder()
                .set(path, JsonObject.newBuilder()
                        .set("grant", toJsonArray(grant))
                        .set("revoke", toJsonArray(revoke))
                        .build())
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

    private static JsonObject localRef(final String entryLabel) {
        return JsonObject.newBuilder()
                .set("entry", entryLabel)
                .build();
    }

    private static PutMatcher putReferences(final PolicyId policyId, final String label,
            final JsonArray references) {
        final String path = ResourcePathBuilder.forPolicy(policyId)
                .policyEntryReferences(label).toString();
        return put(dittoUrl(TestConstants.API_V_2, path), references.toString())
                .withLogging(LOGGER, "References");
    }

}
